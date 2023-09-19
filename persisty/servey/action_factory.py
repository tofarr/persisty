from typing import Iterator

from servey.action.action import Action

from persisty.search_filter.search_filter_factory import search_filter_dataclass_for
from persisty.search_order.search_order_factory import search_order_dataclass_for
from persisty.servey.action_factory_abc import ActionFactoryABC, ROUTE, T
from persisty.store.store_abc import StoreABC


class ActionFactory(ActionFactoryABC[T]):
    # pylint: disable=R0914
    def create_actions(self, store: StoreABC[T]) -> Iterator[Action]:
        from persisty.servey.actions import wrap_links_in_actions

        store_meta = store.get_meta()

        unsecured_store_access = store_meta.store_security.get_potential_access()
        unsecured_store = store_meta.store_security.get_unsecured(store)
        unsecured_store_meta = unsecured_store.get_meta()
        item_type = wrap_links_in_actions(unsecured_store_meta.get_read_dataclass())
        search_filter_type = search_filter_dataclass_for(unsecured_store_meta)
        search_order_type = search_order_dataclass_for(unsecured_store_meta)
        create_input_type = unsecured_store_meta.get_create_dataclass()
        update_input_type = unsecured_store_meta.get_update_dataclass()

        if unsecured_store_access.creatable:
            from persisty.servey.actions import action_for_create

            yield action_for_create(
                store,
                item_type,
                create_input_type,
            )
        if unsecured_store_access.readable:
            from persisty.servey.actions import action_for_read

            yield action_for_read(store, item_type)
        if unsecured_store_access.updatable:
            from persisty.servey.actions import action_for_update

            yield action_for_update(
                store,
                item_type,
                update_input_type,
                search_filter_type,
            )
        if unsecured_store_access.deletable:
            from persisty.servey.actions import action_for_delete

            yield action_for_delete(store)
        if unsecured_store_access.searchable:
            from persisty.servey.actions import action_for_search

            yield action_for_search(
                store,
                item_type,
                search_filter_type,
                search_order_type,
            )
            from persisty.servey.actions import action_for_count

            yield action_for_count(store, search_filter_type)
        if unsecured_store_access.readable:
            from persisty.servey.actions import action_for_read_batch

            yield action_for_read_batch(store, item_type)
        if unsecured_store_access.editable:
            from persisty.servey.actions import action_for_edit_batch

            yield action_for_edit_batch(
                store,
                create_input_type,
                update_input_type,
            )

    # noinspection PyMethodMayBeStatic
    def create_routes(self, store: StoreABC[T]) -> Iterator[ROUTE]:
        return iter(tuple())
