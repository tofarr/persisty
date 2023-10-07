from typing import Iterator

from servey.action.action import Action

from persisty.result import result_dataclass_for
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.search_filter_factory import search_filter_dataclass_for
from persisty.search_order.search_order_factory import search_order_dataclass_for
from persisty.servey import generated
from persisty.servey.action_factory_abc import ActionFactoryABC
from persisty.store_meta import StoreMeta


class ActionFactory(ActionFactoryABC):
    # pylint: disable=R0914
    def create_actions(self, store_meta: StoreMeta) -> Iterator[Action]:
        from persisty.servey.actions import wrap_links_in_actions

        store_meta = store_meta.create_api_meta()
        access = store_meta.store_access
        item_type = wrap_links_in_actions(store_meta.get_read_dataclass())
        result_type = result_dataclass_for(item_type)
        setattr(generated, result_type.__name__, result_type)
        search_filter_type = search_filter_dataclass_for(store_meta)
        search_order_type = search_order_dataclass_for(store_meta)
        create_input_type = store_meta.get_create_dataclass()
        update_input_type = store_meta.get_update_dataclass()

        if access.create_filter is not EXCLUDE_ALL:
            from persisty.servey.actions import action_for_create

            yield action_for_create(
                store_meta,
                result_type,
                create_input_type,
            )
        if access.read_filter is not EXCLUDE_ALL:
            from persisty.servey.actions import action_for_read

            yield action_for_read(store_meta, result_type)
        if access.update_filter is not EXCLUDE_ALL:
            from persisty.servey.actions import action_for_update

            yield action_for_update(
                store_meta,
                result_type,
                update_input_type,
                search_filter_type,
            )
        if access.delete_filter is not EXCLUDE_ALL:
            from persisty.servey.actions import action_for_delete

            yield action_for_delete(store_meta)
        if access.searchable:
            from persisty.servey.actions import action_for_search

            yield action_for_search(
                store_meta,
                result_type,
                search_filter_type,
                search_order_type,
            )
            from persisty.servey.actions import action_for_count

            yield action_for_count(store_meta, search_filter_type)
        if access.read_filter is not EXCLUDE_ALL:
            from persisty.servey.actions import action_for_read_batch

            yield action_for_read_batch(store_meta, result_type)
        if access.editable:
            from persisty.servey.actions import action_for_edit_batch

            yield action_for_edit_batch(
                store_meta,
                create_input_type,
                update_input_type,
            )
