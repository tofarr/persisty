from typing import Iterator

from servey.action.action import Action

from persisty.result import result_dataclass_for
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.search_filter_factory import search_filter_dataclass_for
from persisty.search_order.search_order_factory import search_order_dataclass_for
from persisty.servey import generated
from persisty.servey.action_factory_abc import ActionFactoryABC, T
from persisty.store.store_abc import StoreABC


class ActionFactory(ActionFactoryABC[T]):
    # pylint: disable=R0914
    def create_actions(self, store: StoreABC[T]) -> Iterator[Action]:
        from persisty.servey.actions import wrap_links_in_actions

        store_meta = store.get_meta()

        api_access = store_meta.store_security.get_api_access()
        item_type = wrap_links_in_actions(store_meta.get_read_dataclass())
        result_type = result_dataclass_for(item_type)
        setattr(generated, result_type.__name__, result_type)
        search_filter_type = search_filter_dataclass_for(store_meta)
        search_order_type = search_order_dataclass_for(store_meta)
        create_input_type = store_meta.get_create_dataclass()
        update_input_type = store_meta.get_update_dataclass()

        if api_access.create_filter is not EXCLUDE_ALL:
            from persisty.servey.actions import action_for_create

            yield action_for_create(
                store,
                result_type,
                create_input_type,
            )
        if api_access.read_filter is not EXCLUDE_ALL:
            from persisty.servey.actions import action_for_read

            yield action_for_read(store, result_type)
        if api_access.update_filter is not EXCLUDE_ALL:
            from persisty.servey.actions import action_for_update

            yield action_for_update(
                store,
                result_type,
                update_input_type,
                search_filter_type,
            )
        if api_access.delete_filter is not EXCLUDE_ALL:
            from persisty.servey.actions import action_for_delete

            yield action_for_delete(store)
        if api_access.searchable:
            from persisty.servey.actions import action_for_search

            yield action_for_search(
                store,
                result_type,
                search_filter_type,
                search_order_type,
            )
            from persisty.servey.actions import action_for_count

            yield action_for_count(store, search_filter_type)
        if api_access.read_filter is not EXCLUDE_ALL:
            from persisty.servey.actions import action_for_read_batch

            yield action_for_read_batch(store, result_type)
        if api_access.editable:
            from persisty.servey.actions import action_for_edit_batch

            yield action_for_edit_batch(
                store,
                create_input_type,
                update_input_type,
            )
