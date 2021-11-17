from dataclasses import dataclass, MISSING
from typing import Type, Optional, Callable

from marshy.factory.optional_marshaller_factory import get_optional_type
from marshy.utils import resolve_forward_refs

from persisty.attr.attr_abc import AttrABC, A, B
from persisty.attr.attr_access_control import AttrAccessControl, OPTIONAL
from persisty.deferred.deferred_lookup import DeferredLookup
from persisty.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.selections import Selections


@dataclass
class BelongsToAttr(AttrABC[A, B]):
    name: str
    type: Type[B]
    key_attr: str
    attr_access_control: AttrAccessControl = OPTIONAL

    def __set_name__(self, owner, name):
        self.name = name
        if self.key_attr is None:
            self.key_attr = f'{name}_id'
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
        if value is not MISSING and value.is_save_required():
            return True
        return False

    def resolve(self,
                owner_instance: A,
                sub_selections: Optional[Selections] = None,
                deferred_resolutions: Optional[DeferredResolutionSet] = None):
        local_deferred_resolutions = deferred_resolutions or DeferredResolutionSet()
        self._resolve_value(owner_instance,
                            lambda v: self._callback(owner_instance, v, sub_selections, deferred_resolutions),
                            sub_selections,
                            local_deferred_resolutions)
        if not deferred_resolutions:
            local_deferred_resolutions.resolve()

    def _callback(self,
                  owner_instance: A,
                  value: B,
                  sub_selections: Selections,
                  deferred_resolutions: DeferredResolutionSet):
        value.resolve_all(sub_selections, deferred_resolutions)
        setattr(owner_instance, f'_{self.name}', value)
        setattr(owner_instance, f'_{self.name}_is_save_required', False)

    def _resolve_value(self,
                       owner_instance: A,
                       callback: Callable[[B], None],
                       deferred_resolutions: DeferredResolutionSet):
        key = getattr(owner_instance, self.key_attr)
        if not key:
            callback(None)
            return

        deferred_lookup = self._find_deferred_lookup(deferred_resolutions)
        if deferred_lookup:
            entity = deferred_lookup.resolved.get(key)
            if entity:
                callback(entity)
                return
        else:
            deferred_lookup = DeferredLookup(self._get_entity_type())
            deferred_resolutions.deferred_resolutions.append(deferred_lookup)
        self._add_callback_to_deferred_lookup(key, callback, deferred_lookup)

    def _find_deferred_lookup(self, deferred_resolutions: DeferredResolutionSet) -> Optional[DeferredLookup[A]]:
        entity_type = self._get_entity_type()
        for deferred_resolution in deferred_resolutions.deferred_resolutions:
            if isinstance(deferred_resolution, DeferredLookup) and deferred_resolution.entity_type == entity_type:
                return deferred_resolution

    def _get_entity_type(self) -> Type[B]:
        if self._entity_type:
            # noinspection PyTypeChecker
            return self._entity_type
        resolved_type = resolve_forward_refs(self.type)
        entity_type = get_optional_type(resolved_type) or resolved_type
        self._entity_type = entity_type
        return entity_type

    @staticmethod
    def _add_callback_to_deferred_lookup(key: str, callback: Callable[[B], None],
                                         deferred_lookup: DeferredLookup[A]):
        callbacks = deferred_lookup.to_resolve.get(key)
        if not callbacks:
            callbacks = []
            deferred_lookup.to_resolve[key] = callbacks
        callbacks.append(callback)
