from typing import Optional, Callable, Type, Iterator

from marshy.utils import resolve_forward_refs

from persisty.cache_header import CacheHeader
from persisty.errors import PersistyError
from persisty2.item_filter import AttrFilter, AttrFilterOp
from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.entity_abc import EntityABC
from persisty.obj_graph.resolver.before_destroy import OnDestroy

from persisty.obj_graph.resolver.resolver_abc import ResolverABC, A, B
from persisty.obj_graph.selection_set import SelectionSet
from persisty2.search_filter import SearchFilter


class HasOne(ResolverABC[A, B]):

    def __init__(self,
                 foreign_key_attr: str,
                 on_destroy: OnDestroy = OnDestroy.NO_ACTION,
                 private_name_: Optional[str] = None,
                 resolved_type: Optional[Type[B]] = None):
        super().__init__(private_name_, resolved_type)
        self.foreign_key_attr = foreign_key_attr
        self.on_destroy = on_destroy
        self._entity_type: Type[B] = None
        self.is_overridden_name = None

    def __set_name__(self, owner, name):
        super().__set_name__(owner, name)
        if self.is_overridden_name is None:
            self.is_overridden_name = f'is_{name}_overridden'

    def __set__(self, owner_instance: A, value: B):
        super().__set__(owner_instance, value)
        setattr(owner_instance, self.is_overridden_name, True)

    def resolve_value(self, owner_instance: A, callback: Callable[[B], None], sub_selections: Optional[SelectionSet],
                      deferred_resolutions: Optional[DeferredResolutionSet] = None):
        entity = self._find(owner_instance)
        if entity is None:
            callback(None)
            return
        if sub_selections:
            entity.resolve_all(sub_selections, deferred_resolutions)
        callback(entity)

    def unresolve(self, owner_instance: A):
        super().unresolve(owner_instance)
        setattr(owner_instance, self.is_overridden_name, False)

    def _find(self, owner_instance: A):
        entities = self._search(owner_instance)
        if entities is None:
            return None
        entity = next(entities, None)
        return entity

    def _search(self, owner_instance: A):
        key = owner_instance.get_key()
        if key is None:
            return None
        search_filter = SearchFilter(AttrFilter(self.foreign_key_attr, AttrFilterOp.eq, key))
        entities = self._get_entity_type().search(search_filter)
        return entities

    def _get_entity_type(self):
        entity_type = self._entity_type
        if entity_type is None:
            entity_type = resolve_forward_refs(self.resolved_type)
            if not issubclass(entity_type, EntityABC):
                raise PersistyError(f'not_a_subclass:{entity_type}:{EntityABC}')
            self._entity_type = entity_type
        return entity_type

    def is_overridden(self, owner_instance: A) -> bool:
        return getattr(owner_instance, self.is_overridden_name, False)

    def before_destroy(self, owner_instance: A):
        setattr(owner_instance, self.is_overridden_name, False)
        if self.on_destroy == OnDestroy.CASCADE:
            entity = self._find(owner_instance)
            if entity:
                entity.destroy()
        elif self.on_destroy == OnDestroy.NULLIFY:
            entity = self._find(owner_instance)
            if entity:
                setattr(entity, self.foreign_key_attr, None)
                entity.update()

    def after_create(self, owner_instance: A):
        if self.is_overridden(owner_instance):
            setattr(owner_instance, self.is_overridden_name, False)
            entity = getattr(owner_instance, self.private_name)
            if entity:
                key = owner_instance.get_key()
                setattr(entity, self.foreign_key_attr, key)
                entity.save()

    def after_update(self, owner_instance: A):
        if self.is_overridden(owner_instance):
            setattr(owner_instance, self.is_overridden_name, False)
            existing_by_key = {e.get_key(): e for e in self._search(owner_instance)}
            key = owner_instance.get_key()
            entity = getattr(owner_instance, self.private_name)
            if entity:
                setattr(entity, self.foreign_key_attr, key)
                entity.save()
                foreign_key = entity.get_key()
                existing_by_key.pop(foreign_key, None)
            for e in existing_by_key.values():
                e.destroy()

    def get_cache_headers(self, owner_instance: A, selections: SelectionSet) -> Iterator[CacheHeader]:
        entity = getattr(owner_instance, self.name)
        yield entity.get_cache_header(selections)
