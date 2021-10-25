import importlib
from dataclasses import dataclass
from typing import Union, Type, Optional, Callable

from persisty.obj_graph.deferred.deferred_lookup import DeferredLookup
from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.resolver.entity_resolver_abc import EntityResolverABC, T
from persisty.obj_graph.selection_set import SelectionSet


@dataclass(frozen=True)
class BelongsTo(EntityResolverABC[T]):
    key_attr: str

    def resolve(self,
                owner: T,
                attr_name: str,
                sub_selections: Optional[SelectionSet],
                deferred_resolutions: DeferredResolutionSet):
        key = getattr(owner, self.key_attr)
        if not key:
            setattr(owner, attr_name, None)
            return

        def callback(resolved_entity: T):
            setattr(owner, attr_name, resolved_entity)
            resolved_entity.resolve_all(sub_selections, deferred_resolutions)

        deferred_lookup = self._find_deferred_lookup(deferred_resolutions)
        if deferred_lookup:
            entity = deferred_lookup.resolved.get(key)
            if entity:
                callback(entity)
                return
        else:
            deferred_lookup = DeferredLookup(self.get_entity_type())
            deferred_resolutions.deferred_resolutions.append(deferred_lookup)
        self._add_callback_to_deferred_lookup(key, callback, deferred_lookup)

    def _find_deferred_lookup(self, deferred_resolutions: DeferredResolutionSet) -> Optional[DeferredLookup[T]]:
        entity_type = self.get_entity_type()
        for deferred_resolution in deferred_resolutions.deferred_resolutions:
            if isinstance(deferred_resolution, DeferredLookup) and deferred_resolution.item_type == entity_type:
                return deferred_resolution

    def _add_callback_to_deferred_lookup(self, key: str, callback: Callable[[T], None],
                                         deferred_lookup: DeferredLookup[T]):
        callbacks = deferred_lookup.to_resolve.get(key)
        if not callbacks:
            callbacks = []
            deferred_lookup.to_resolve[key] = callbacks
        callbacks.append(callback)
