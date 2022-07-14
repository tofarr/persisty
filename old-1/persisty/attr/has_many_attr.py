from collections import Iterable
from dataclasses import dataclass, MISSING
from typing import Type, Optional, Callable, List

import typing_inspect
from marshy.utils import resolve_forward_refs
from schemey.any_of_schema import optional_schema
from schemey.array_schema import ArraySchema
from schemey.schema_abc import SchemaABC

from persisty.attr.attr_abc import AttrABC, A, B
from persisty.attr.attr_access_control import AttrAccessControl, OPTIONAL
from persisty.attr.on_destroy import OnDestroy
from persisty.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.entity.entity_abc import EntityABC
from persisty.entity.selections import Selections
from persisty.errors import PersistyError
from persisty.item_filter import AttrFilterOp, AttrFilter
from persisty.storage.storage_filter import StorageFilter
from persisty.util import to_snake_case


@dataclass
class HasManyAttr(AttrABC[A, List[B]]):
    name: str = None
    type: Type[B] = None
    foreign_key_attr: str = None
    attr_access_control: AttrAccessControl = OPTIONAL
    on_destroy: OnDestroy = OnDestroy.NOOP

    def __set_name__(self, owner, name):
        self.name = name
        if self.type is None:
            self.type = owner.__annotations__.get(self.name)
            if not self.type:
                raise ValueError(f'missing_annotation:{owner}:{self.name}')

    def __get__(self, owner_instance: A, owner_type: Type[A]) -> B:
        value = getattr(owner_instance, f'_{self.name}', MISSING)
        if value is MISSING:
            self.resolve(owner_instance)
            value = getattr(owner_instance, f'_{self.name}')
        return value

    def __set__(self, owner_instance: A, value: B):
        setattr(owner_instance, f'_{self.name}', value)
        setattr(owner_instance, f'_{self.name}_is_save_required', True)

    def unresolve(self, owner_instance: A):
        setattr(owner_instance, f'_{self.name}', MISSING)
        setattr(owner_instance, f'_{self.name}_is_save_required', False)

    def is_resolved(self, owner_instance: A) -> bool:
        is_resolved = getattr(owner_instance, f'_{self.name}', MISSING) != MISSING
        return is_resolved

    def is_save_required(self, owner_instance: A) -> bool:
        if getattr(owner_instance, f'_{self.name}_is_save_required', False):
            return True
        value = getattr(owner_instance, f'_{self.name}', MISSING)
        if value is MISSING:
            return False
        entity_needs_save = next((v for v in value if v.is_save_required()), None)
        return bool(entity_needs_save)

    def resolve(self,
                owner_instance: A,
                sub_selections: Optional[Selections] = None,
                deferred_resolutions: Optional[DeferredResolutionSet] = None):
        local_deferred_resolutions = deferred_resolutions or DeferredResolutionSet()
        self._resolve_value(owner_instance,
                            lambda v: self._callback(owner_instance, v, sub_selections, local_deferred_resolutions))
        if not deferred_resolutions:
            local_deferred_resolutions.resolve()

    def _callback(self,
                  owner_instance: A,
                  value: List[B],
                  sub_selections: Selections,
                  deferred_resolutions: DeferredResolutionSet):
        for v in value:
            v.resolve_all(sub_selections, deferred_resolutions)
        setattr(owner_instance, f'_{self.name}', value)
        setattr(owner_instance, f'_{self.name}_is_save_required', False)

    def _resolve_value(self,
                       owner_instance: A,
                       callback: Callable[[B], None]):
        key = owner_instance.get_storage().meta.key_config.get_key(owner_instance)
        foreign_key_attr = self.foreign_key_attr
        if foreign_key_attr is None:
            foreign_key_attr = f'{to_snake_case(owner_instance.get_entity_config().item_class.__name__)}_id'
        storage_filter = StorageFilter(AttrFilter(foreign_key_attr, AttrFilterOp.eq, key))
        entity_type = self._get_entity_type()
        entities = list(entity_type.search(storage_filter))
        callback(entities)

    def _get_entity_type(self):
        entity_type = getattr(self, '_entity_type', None)
        if entity_type:
            return entity_type
        resolved_type = resolve_forward_refs(self.type)
        if typing_inspect.get_origin(resolved_type) not in [list, Iterable]:
            raise PersistyError(f'type_should_be_list:{self.name}')
        entity_type = typing_inspect.get_args(resolved_type)[0]
        if not issubclass(entity_type, EntityABC):
            raise PersistyError(f'not_an_entity:{self.name}:{entity_type}')
        return entity_type

    @property
    def schema(self) -> SchemaABC[B]:
        schema = optional_schema(ArraySchema(self._get_entity_type().get_schema()))
        return schema

    def before_destroy(self, owner_instance: A):
        if self.on_destroy == OnDestroy.NULLIFY:
            values = getattr(owner_instance, self.name)
            for v in values:
                setattr(v, self.foreign_key_attr, None)
                v.save()
        elif self.on_destroy == OnDestroy.CASCADE:
            values = getattr(owner_instance, self.name)
            for v in values:
                v.destroy()

    def after_create(self, owner_instance: A):
        self.after_save(owner_instance)

    def after_update(self, owner_instance: A):
        self.after_save(owner_instance)

    def after_save(self, owner_instance: A):
        # TODO: It may be possible to batch this operation for greater efficiency...
        value = getattr(owner_instance, f'_{self.name}', MISSING)
        if value is MISSING:
            return
        key = owner_instance.get_storage().meta.key_config.get_key(owner_instance)
        for v in value:
            setattr(v, self.foreign_key_attr, key)
            if v.is_save_required:
                v.save()
