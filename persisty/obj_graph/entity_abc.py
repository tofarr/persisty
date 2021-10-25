import dataclasses
from abc import ABC
from typing import Optional, TypeVar, Generic, Union, ForwardRef, List, Iterator

from persisty import get_default_persisty_context
from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.resolver.resolver_descriptor import ResolverDescriptor, NOT_INITIALIZED
from persisty.obj_graph.selection_set import SelectionSet
from persisty.page import Page
from persisty.errors import PersistyError
from persisty.repo_abc import RepoABC

T = TypeVar('T')
F = TypeVar('F')

REMOTE_VALUES_ATTR = '__remote_values__'
ITEMS_ATTR = 'items'
KEY_ATTR = '__key_attr__'
ID = 'id'


class EntityABC(Generic[T, F], ABC):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)  # forwards all unused arguments
        self.__remote_values__ = NOT_INITIALIZED

    @classmethod
    def read(cls,
             key: str,
             selections: Optional[SelectionSet] = None,
             deferred_resolutions: Optional[DeferredResolutionSet] = None
             ) -> Union[ForwardRef('EntityABC'), T]:
        repo = cls.get_repo()
        remote_values = repo.read(key)
        if remote_values is None:
            raise PersistyError(f'missing_item:{key}')
        entity = cls._entity(remote_values, selections, deferred_resolutions)
        return entity

    @classmethod
    def read_all(cls,
                 keys: Iterator[str],
                 error_on_missing: bool = True,
                 selections: Optional[SelectionSet] = None,
                 deferred_resolutions: Optional[DeferredResolutionSet] = None
                 ) -> Iterator[T]:
        repo = cls.get_repo()
        items = repo.read_all(keys, error_on_missing)
        entities = (cls._entity(item, selections, deferred_resolutions) for item in items)
        return entities

    @classmethod
    def _entity(cls,
                remote_values: T,
                selections: Optional[SelectionSet] = None,
                deferred_resolutions: Optional[DeferredResolutionSet] = None):
        entity = cls(**remote_values.__dict__)
        setattr(entity, REMOTE_VALUES_ATTR, remote_values)
        if selections:
            local_deferred_resolutions = deferred_resolutions or []
            for d in cls.get_resolver_descriptors():
                if selections.get_selections(d.name):
                    d.resolve(entity, d.name, )
            if not deferred_resolutions:
                local_deferred_resolutions.resolve()
        return entity

    @classmethod
    def search(cls, search_filter: Optional[F] = None):
        repo = cls.get_repo()
        items = repo.search(search_filter)
        entities = (cls._entity(item) for item in items)
        return entities

    @classmethod
    def count(cls, search_filter):
        repo = cls.get_repo()
        count = repo.count(search_filter)
        return count

    @classmethod
    def paginated_search(cls,
                         search_filter: Optional[F] = None,
                         page_key: Optional[str] = None,
                         limit: int = 20,
                         selections: Optional[SelectionSet] = None,
                         deferred_resolutions: Optional[DeferredResolutionSet] = None):
        repo = cls.get_repo()
        local_deferred_resolutions = deferred_resolutions or DeferredResolutionSet()
        page = repo.paginated_search(search_filter, page_key, limit)
        entities = [cls._entity(item, selections, local_deferred_resolutions) for item in page.items]
        if not deferred_resolutions:
            local_deferred_resolutions.resolve()
        wrapped_page = Page(entities, page.next_page_key)
        return wrapped_page

    @classmethod
    def get_repo(cls) -> RepoABC[T, F]:
        repo_name = cls._get_wrapped_class().__name__
        if not hasattr(cls, '__persisty_context__'):
            cls.__persisty_context__ = get_default_persisty_context()
        repo = cls.__persisty_context__.get_repo(repo_name)
        return repo

    @classmethod
    def _get_wrapped_class(cls):
        if not hasattr(cls, '__wrapped_class__'):
            cls.__wrapped_class__ = next(s for s in cls.__mro__[1:] if dataclasses.is_dataclass(s))
        return cls.__wrapped_class__

    @classmethod
    def get_resolver_descriptors(cls) -> List[ResolverDescriptor]:
        cls_annotations = cls.__dict__.get('__annotations__', {})
        resolver_descriptors = [a for a in cls_annotations.values() if isinstance(a, ResolverDescriptor)]
        return resolver_descriptors

    @property
    def is_save_required(self):
        return self != self.__remote_values__

    def get_key(self):
        key = self.get_repo().get_key(self)
        return key

    @property
    def is_existing(self):
        repo = self.get_repo()
        key = repo.get_key(self)
        if key is None:
            return False
        if self.__remote_values__ is NOT_INITIALIZED:
            self.__remote_values__ = repo.read(key)
        return bool(self.__remote_values__)

    def load(self):
        repo = self.get_repo()
        key = repo.get_key(self.__remote_values__ or self)
        if key is None:
            raise PersistyError('missing_key')
        self.__remote_values__ = repo.read(key)
        if not self.__remote_values__:
            raise PersistyError(f'no_such_entity:{key}')
        self._copy_from_remote()

    def _copy_from_remote(self):
        for field in dataclasses.fields(self.__remote_values__):
            setattr(self, field.name, getattr(self.__remote_values__, field.name))

    def save(self):
        if not self.is_save_required:
            return
        if self.is_existing:
            return self.update()
        else:
            return self.create()

    def create(self):
        repo = self.get_repo()
        key = repo.create(self)
        setattr(self, self._get_key_attr(), key)
        self._build_remote_from_local()

    def _get_key_attr(self):
        return self.__key_attr__ if hasattr(self, '__key_attr__') else 'id'

    def _build_remote_from_local(self):
        # noinspection PyDataclass
        init_fields = (f for f in dataclasses.fields(self._get_wrapped_class()) if f.init)
        kwargs = {f.name: getattr(self, f.name) for f in init_fields}
        self.__remote_values__ = self._get_wrapped_class()(**kwargs)

    def update(self):
        repo = self.get_repo()
        repo.update(self)
        self._build_remote_from_local()

    def destroy(self):
        repo = self.get_repo()
        key = repo.get_key(self)
        repo.destroy(key)
        self.__remote_values__ = None

    def resolve_all(self,
                    selections: Optional[SelectionSet],
                    deferred_resolutions: Optional[DeferredResolutionSet] = None):
        if selections is None:
            return
        local_deferred_resolutions = [] if deferred_resolutions is None else deferred_resolutions
        for resolver_descriptor in self.get_resolver_descriptors():
            resolver_descriptor.resolve(self, selections, local_deferred_resolutions)
        if deferred_resolutions is None:
            local_deferred_resolutions.resolve()
