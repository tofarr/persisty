from typing import Optional, Callable, Type

from marshy.factory.optional_marshaller_factory import get_optional_type
from marshy.utils import resolve_forward_refs

from persisty.obj_graph.deferred.deferred_lookup import DeferredLookup
from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet

from persisty.obj_graph.resolver.resolver_abc import ResolverABC, A, B
from persisty.obj_graph.selection_set import SelectionSet


class BelongsTo(ResolverABC[A, B]):

    def __init__(self,
                 key_attr: Optional[str] = None,
                 private_name_: Optional[str] = None,
                 resolved_type: Optional[Type[B]] = None):
        super().__init__(private_name_, resolved_type)
        self.key_attr = key_attr
        self._entity_type: Type[B] = None

    def __set_name__(self, owner, name):
        super().__set_name__(owner, name)
        if self.key_attr is None:
            self.key_attr = f'{name}_id'

    def resolve_value(self,
                      owner_instance: A,
                      callback: Callable[[B], None],
                      sub_selections: Optional[SelectionSet],
                      deferred_resolutions: Optional[DeferredResolutionSet] = None):

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
        resolved_type = resolve_forward_refs(self.resolved_type)
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

    def __set__(self, owner_instance: A, value: B):
        super().__set__(owner_instance, value)
        key = None if value is None else value.get_key()
        setattr(owner_instance, self.key_attr, key)

    def before_create(self, owner_instance: A):
        if self.is_resolved(owner_instance):
            value = getattr(owner_instance, self.private_name)
            if value and value.is_save_required:
                value.save()
                key = None if value is None else value.get_key()
                setattr(owner_instance, self.key_attr, key)

    def before_update(self, owner_instance: A):
        self.before_create(owner_instance)
