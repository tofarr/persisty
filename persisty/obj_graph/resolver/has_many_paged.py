from dataclasses import dataclass
from typing import Optional, Callable, TypeVar

from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.resolver.entity_resolver_abc import EntityResolverABC, T
from persisty.obj_graph.selection_set import SelectionSet

F = TypeVar('F')


@dataclass(frozen=True)
class HasManyPaged(EntityResolverABC[T]):
    search_filter_factory: Callable[[T], F]
    inverse_attr: Optional[str] = None
    limit: int = 20

    def resolve(self,
                owner: T,
                attr_name: str,
                sub_selections: Optional[SelectionSet],
                deferred_resolutions: DeferredResolutionSet):
        search_filter = self.search_filter_factory(owner)
        if search_filter is None:
            setattr(owner, attr_name, None)
            return
        entity_type = self.get_entity_type()
        page = entity_type.paginated_search(search_filter, limit=self.limit)
        if sub_selections:
            item_selections = sub_selections.get_selections('items')
            if item_selections:
                for entity in page.items:
                    entity.resolve_all(item_selections, deferred_resolutions)
        if self.inverse_attr:
            for entity in page.items:
                setattr(entity, self.inverse_attr, owner)
        setattr(owner, attr_name, page)
