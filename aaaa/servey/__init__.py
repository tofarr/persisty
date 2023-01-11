from typing import Optional, Dict, Callable, Type, List

from marshy import get_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller_context import MarshallerContext
from servey.action.action import action, get_action
from servey.security.authorization import Authorization
from servey.trigger.web_trigger import WebTrigger, WebTriggerMethod

from aaaa.search_filter.include_all import INCLUDE_ALL
from aaaa.search_filter.search_filter_factory import (
    search_filter_dataclass_for,
    SearchFilterFactoryABC,
)
from aaaa.search_order.search_order_factory import (
    search_order_dataclass_for,
    SearchOrderFactoryABC,
)
from aaaa.secured.secured_store_factory_abc import SecuredStoreFactoryABC
from aaaa.batch_edit import batch_edit_dataclass_for, BatchEdit
from aaaa.batch_edit_result import batch_edit_result_dataclass_for


def add_actions_for_all_store_factories(
    target: Dict, marshaller_context: Optional[MarshallerContext] = None
):
    if not marshaller_context:
        marshaller_context = get_default_context()
    from aaaa.finder.store_factory_finder_abc import find_secured_store_factories
    for store_factory in find_secured_store_factories():
        add_actions_for_store_factory(store_factory, target, marshaller_context)


def add_actions_for_store_factory(
    store_factory: SecuredStoreFactoryABC,
    target: Dict,
    marshaller_context: MarshallerContext,
):
    store_meta = store_factory.get_store_meta()
    store_access = store_meta.store_access
    item_type = store_meta.get_read_dataclass()
    marshaller = marshaller_context.get_marshaller(item_type)
    search_filter_type = search_filter_dataclass_for(store_meta)
    search_order_type = search_order_dataclass_for(store_meta)
    create_input_type = store_meta.get_create_dataclass()
    create_input_type_marshaller = marshaller_context.get_marshaller(create_input_type)
    update_input_type = store_meta.get_update_dataclass()
    update_input_type_marshaller = marshaller_context.get_marshaller(update_input_type)

    actions = []
    if store_access.creatable:
        actions.append(action_for_create(
            store_factory,
            marshaller,
            item_type,
            create_input_type,
            create_input_type_marshaller,
        ))
    if store_access.readable:
        actions.append(action_for_read(store_factory, marshaller, item_type))
    if store_access.updatable:
        actions.append(action_for_update(
            store_factory,
            marshaller,
            item_type,
            update_input_type,
            update_input_type_marshaller,
            search_filter_type,
        ))
    if store_access.deletable:
        actions.append(action_for_delete(store_factory))
    if store_access.searchable:
        actions.append(action_for_search(
            store_factory,
            marshaller,
            search_filter_type,
            search_order_type,
        ))
        actions.append(action_for_count(store_factory, search_filter_type))
    if store_access.readable:
        actions.append(action_for_read_batch(store_factory, marshaller, item_type))
    if store_access.editable:
        actions.append(action_for_edit_batch(
            store_factory,
            create_input_type,
            create_input_type_marshaller,
            update_input_type,
            update_input_type_marshaller,
        ))

    for action_ in actions:
        target[get_action(action_).name] = action_


def action_for_create(
    store_factory: SecuredStoreFactoryABC,
    marshaller: MarshallerABC,
    item_type: Type,
    create_input_type: Type,
    create_input_type_marshaller: MarshallerABC,
) -> Callable:
    store_meta = store_factory.get_store_meta()

    @action(
        name=f"{store_meta.name}_create",
        description=f"Create and return an item in {store_meta.name}",
        triggers=(WebTrigger(WebTriggerMethod.POST, "/actions/" + store_meta.name),),
    )
    def create(
        item: create_input_type, authorization: Optional[Authorization] = None
    ) -> Optional[item_type]:
        dumped = create_input_type_marshaller.dump(item)
        store = store_factory.create(authorization)
        created = store.create(dumped)
        if created:
            return marshaller.load(created)

    return create


def action_for_read(
    store_factory: SecuredStoreFactoryABC, marshaller: MarshallerABC, item_type: Type
) -> Callable:
    store_meta = store_factory.get_store_meta()

    @action(
        name=f"{store_meta.name}_read",
        description=f"Read an item from {store_meta.name} given a key",
        triggers=(
            WebTrigger(
                WebTriggerMethod.GET, "/actions/" + store_meta.name + "/{key}"
            ),
        ),
        cache_control=store_meta.cache_control,
    )
    def read(
        key: str, authorization: Optional[Authorization] = None
    ) -> Optional[item_type]:
        store = store_factory.create(authorization)
        result = store.read(key)
        if result:
            return marshaller.load(result)

    return read


def action_for_update(
    store_factory: SecuredStoreFactoryABC,
    marshaller: MarshallerABC,
    item_type: Type,
    update_input_type: Type,
    update_input_type_marshaller: MarshallerABC,
    search_filter_type: Type[SearchFilterFactoryABC],
) -> Callable:
    store_meta = store_factory.get_store_meta()

    @action(
        name=f"{store_meta.name}_update",
        description=f"Update and return an item in {store_meta.name}",
        triggers=(
            WebTrigger(
                WebTriggerMethod.PATCH, "/actions/" + store_meta.name + "/{key}"
            ),
        ),
    )
    def update(
        key: str,
        item: update_input_type,
        precondition: Optional[search_filter_type] = None,
        authorization: Optional[Authorization] = None,
    ) -> Optional[item_type]:
        dumped = update_input_type_marshaller.dump(item)
        store_meta.key_config.from_key_str(key, dumped)
        store = store_factory.create(authorization)
        search_filter = precondition.to_search_filter() if precondition else INCLUDE_ALL
        updated = store.update(dumped, search_filter)
        if updated:
            return marshaller.load(item)

    return update


def action_for_delete(store_factory: SecuredStoreFactoryABC) -> Callable:
    store_meta = store_factory.get_store_meta()

    @action(
        name=f"{store_meta.name}_delete",
        description=f"Delete an item in {store_meta.name}",
        triggers=(
            WebTrigger(
                WebTriggerMethod.DELETE, "/actions/" + store_meta.name + "/{key}"
            ),
        ),
    )
    def delete(key: str, authorization: Optional[Authorization] = None) -> bool:

        store = store_factory.create(authorization)
        result = store.delete(key)
        return result

    return delete


def action_for_search(
    store_factory: SecuredStoreFactoryABC,
    marshaller: MarshallerABC,
    search_filter_type: Type[SearchFilterFactoryABC],
    search_order_type: Type[SearchOrderFactoryABC],
) -> Callable:
    store_meta = store_factory.get_store_meta()
    # noinspection PyTypeChecker
    result_set_type = store_meta.get_result_set_dataclass()

    @action(
        name=f"{store_meta.name}_search",
        description=f"Run a search in {store_meta.name}",
        triggers=(
            WebTrigger(WebTriggerMethod.GET, f"/actions/{store_meta.name}-search"),
        ),
    )
    def search(
        search_filter: Optional[search_filter_type] = None,
        search_order: Optional[search_order_type] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
        authorization: Optional[Authorization] = None,
    ) -> result_set_type:
        store = store_factory.create(authorization)
        search_filter = (
            search_filter.to_search_filter() if search_filter else INCLUDE_ALL
        )
        search_order = search_order.to_search_order() if search_order else None
        result_set = store.search(search_filter, search_order, page_key, limit)

        # noinspection PyArgumentList
        result_set = result_set_type(
            results=[marshaller.load(r) for r in result_set.results],
            next_page_key=result_set.next_page_key,
        )
        return result_set

    return search


def action_for_count(
    store_factory: SecuredStoreFactoryABC,
    search_filter_type: Type[SearchFilterFactoryABC],
) -> Callable:
    store_meta = store_factory.get_store_meta()

    @action(
        name=f"{store_meta.name}_count",
        description=f"Get a count from {store_meta.name}",
        triggers=(
            WebTrigger(WebTriggerMethod.GET, f"/actions/{store_meta.name}-count"),
        ),
    )
    def count(
        search_filter: Optional[search_filter_type] = None,
        authorization: Optional[Authorization] = None,
    ) -> int:
        store = store_factory.create(authorization)
        search_filter = (
            search_filter.to_search_filter() if search_filter else INCLUDE_ALL
        )
        result = store.count(search_filter)
        return result

    return count


def action_for_read_batch(
    store_factory: SecuredStoreFactoryABC, marshaller: MarshallerABC, item_type: Type
) -> Callable:
    store_meta = store_factory.get_store_meta()

    @action(
        name=f"{store_meta.name}_read_batch",
        description=f"Read a batch of items from {store_meta.name} given keys",
        triggers=(
            WebTrigger(
                WebTriggerMethod.GET, "/actions/" + store_meta.name + "-batch"
            ),
        ),
        cache_control=store_meta.cache_control,
    )
    def read_batch(
        keys: List[str], authorization: Optional[Authorization] = None
    ) -> List[Optional[item_type]]:
        store = store_factory.create(authorization)
        results = store.read_batch(keys)
        results = [marshaller.load(r) if r else None for r in results]
        return results

    return read_batch


def action_for_edit_batch(
    store_factory: SecuredStoreFactoryABC,
    create_input_type: Type,
    create_input_type_marshaller: MarshallerABC,
    update_input_type: Type,
    update_input_type_marshaller: MarshallerABC,
) -> Callable:
    store_meta = store_factory.get_store_meta()
    batch_edit_type = batch_edit_dataclass_for(
        store_meta.name.title() + "BatchEdit", create_input_type, update_input_type
    )
    batch_edit_result_type = batch_edit_result_dataclass_for(batch_edit_type)

    @action(
        name=f"{store_meta.name}_edit_batch",
        description=f"Perform a batch of edits against {store_meta.name}",
        triggers=(
            WebTrigger(
                WebTriggerMethod.PATCH, "/actions/" + store_meta.name + "-batch"
            ),
        ),
    )
    def edit_batch(
        edits: List[batch_edit_type], authorization: Optional[Authorization] = None
    ) -> List[batch_edit_result_type]:
        store = store_factory.create(authorization)
        internal_edits = [
            BatchEdit(
                create_item=create_input_type_marshaller.dump(e.create_item)
                if e.create_item
                else None,
                update_item=update_input_type_marshaller.dump(e.update_item)
                if e.update_item
                else None,
                delete_key=e.delete_key,
            )
            for e in edits
        ]
        results = store.edit_batch(internal_edits)
        results = [
            batch_edit_result_type(
                edit=edit,
                success=result.success,
                code=result.code,
                details=result.details,
            )
            for edit, result in zip(edits, results)
        ]
        return results

    return edit_batch
