from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Optional, Generic, Iterator

from servey.action.action import Action
from servey.security.authorization import Authorization

from persisty.search_filter.search_filter_factory import search_filter_dataclass_for
from persisty.search_order.search_order_factory import search_order_dataclass_for
from persisty.store.store_abc import StoreABC, T
from persisty.store_meta import StoreMeta

ROUTE = "starlette.routing.Route"


@dataclass
class StoreFactoryABC(Generic[T], ABC):
    """
    Factory for store objects which allows defining access control rules for store.
    """

    @abstractmethod
    def get_meta(self) -> StoreMeta:
        """Get the meta for the store"""

    @abstractmethod
    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[T]]:
        """Create a new store instance"""

    def create_actions(self) -> Iterator[Action]:
        """Create actions for this factory"""
        from persisty.servey.actions import wrap_links_in_actions

        store_meta = self.get_meta()
        store_access = store_meta.store_access
        item_type = wrap_links_in_actions(store_meta.get_read_dataclass())
        search_filter_type = search_filter_dataclass_for(store_meta)
        search_order_type = search_order_dataclass_for(store_meta)
        create_input_type = store_meta.get_create_dataclass()
        update_input_type = store_meta.get_update_dataclass()

        if store_access.creatable:
            from persisty.servey.actions import action_for_create

            yield action_for_create(
                self,
                item_type,
                create_input_type,
            )
        if store_access.readable:
            from persisty.servey.actions import action_for_read

            yield action_for_read(self, item_type)
        if store_access.updatable:
            from persisty.servey.actions import action_for_update

            yield action_for_update(
                self,
                item_type,
                update_input_type,
                search_filter_type,
            )
        if store_access.deletable:
            from persisty.servey.actions import action_for_delete

            yield action_for_delete(self)
        if store_access.searchable:
            from persisty.servey.actions import action_for_search

            yield action_for_search(
                self,
                item_type,
                search_filter_type,
                search_order_type,
            )
            from persisty.servey.actions import action_for_count

            yield action_for_count(self, search_filter_type)
        if store_access.readable:
            from persisty.servey.actions import action_for_read_batch

            yield action_for_read_batch(self, item_type)
        if store_access.editable:
            from persisty.servey.actions import action_for_edit_batch

            yield action_for_edit_batch(
                self,
                create_input_type,
                update_input_type,
            )

    # noinspection PyMethodMayBeStatic
    def create_routes(self) -> Iterator[ROUTE]:
        """Create routes for this factory"""
        return iter(tuple())
