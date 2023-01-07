from dataclasses import dataclass
from typing import Optional, Dict, Callable, Type, List

from marshy import get_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller_context import MarshallerContext
from servey.action.action import action, get_action
from servey.security.authorization import Authorization
from servey.trigger.web_trigger import WebTrigger, WebTriggerMethod

from persisty.finder.storage_factory_finder_abc import find_storage_factories
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_factory import (
    search_filter_dataclass_for,
    SearchFilterFactoryABC,
)
from persisty.search_order.search_order_factory import (
    search_order_dataclass_for,
    SearchOrderFactoryABC,
)
from persisty.servey import output
from persisty.servey.input import input_type_for_create, input_type_for_update
from persisty.storage.batch_edit import batch_edit_dataclass_for, BatchEdit
from persisty.storage.batch_edit_result import batch_edit_result_dataclass_for
from persisty.storage.result_set import result_set_dataclass_for
from persisty.storage.storage_factory_abc import StorageFactoryABC
from persisty.storage.storage_meta import StorageMeta


def add_actions_for_all_storage_factories(
    target: Dict,
    marshaller_context: Optional[MarshallerContext] = None
):
    if not marshaller_context:
        marshaller_context = get_default_context()
    for storage_factory in find_storage_factories():
        add_actions_for_storage_factory(
            storage_factory, target, marshaller_context
        )


def add_actions_for_storage_factory(
    storage_factory: StorageFactoryABC,
    target: Dict,
    marshaller_context: MarshallerContext,
):
    storage_meta = storage_factory.get_storage_meta()
    item_type = get_item_type(storage_meta)
    marshaller = marshaller_context.get_marshaller(item_type)
    search_filter_type = search_filter_dataclass_for(storage_meta)
    search_order_type = search_order_dataclass_for(storage_meta)
    create_input_type = input_type_for_create(storage_meta)
    create_input_type_marshaller = marshaller_context.get_marshaller(create_input_type)
    update_input_type = input_type_for_update(storage_meta)
    update_input_type_marshaller = marshaller_context.get_marshaller(update_input_type)

    actions = [
        action_for_create(
            storage_factory,
            marshaller,
            item_type,
            create_input_type,
            create_input_type_marshaller,
        ),
        action_for_read(storage_factory, marshaller, item_type),
        action_for_update(
            storage_factory,
            marshaller,
            item_type,
            update_input_type,
            update_input_type_marshaller,
            search_filter_type,
        ),
        action_for_delete(storage_factory),
        action_for_search(
            storage_factory, marshaller, item_type, search_filter_type, search_order_type
        ),
        action_for_count(storage_factory, search_filter_type),
        action_for_read_batch(storage_factory, marshaller, item_type),
        action_for_edit_batch(
            storage_factory,
            create_input_type,
            create_input_type_marshaller,
            update_input_type,
            update_input_type_marshaller,
        ),
    ]
    for action_ in actions:
        target[get_action(action_).name] = action_


def action_for_create(
    storage_factory: StorageFactoryABC,
    marshaller: MarshallerABC,
    item_type: Type,
    create_input_type: Type,
    create_input_type_marshaller: MarshallerABC,
) -> Callable:
    storage_meta = storage_factory.get_storage_meta()

    @action(
        name=f"{storage_meta.name}_create",
        description=f"Create and return an item in {storage_meta.name}",
        triggers=(WebTrigger(WebTriggerMethod.PATCH, "/actions/" + storage_meta.name),),
    )
    def create(
        item: create_input_type, authorization: Optional[Authorization] = None
    ) -> Optional[item_type]:
        dumped = create_input_type_marshaller.dump(item)
        storage = storage_factory.create(authorization)
        created = storage.create(dumped)
        if created:
            return marshaller.load(created)

    return create


def action_for_read(
    storage_factory: StorageFactoryABC, marshaller: MarshallerABC, item_type: Type
) -> Callable:
    storage_meta = storage_factory.get_storage_meta()

    @action(
        name=f"{storage_meta.name}_read",
        description=f"Read an item from {storage_meta.name} given a key",
        triggers=(
            WebTrigger(
                WebTriggerMethod.GET, "/actions/" + storage_meta.name + "/{key}"
            ),
        ),
        cache_control=storage_meta.cache_control
    )
    def read(
        key: str, authorization: Optional[Authorization] = None
    ) -> Optional[item_type]:
        storage = storage_factory.create(authorization)
        result = storage.read(key)
        if result:
            return marshaller.load(result)

    return read


def action_for_update(
    storage_factory: StorageFactoryABC,
    marshaller: MarshallerABC,
    item_type: Type,
    update_input_type: Type,
    update_input_type_marshaller: MarshallerABC,
    search_filter_type: Type[SearchFilterFactoryABC],
) -> Callable:
    storage_meta = storage_factory.get_storage_meta()

    @action(
        name=f"{storage_meta.name}_update",
        description=f"Update and return an item in {storage_meta.name}",
        triggers=(
            WebTrigger(
                WebTriggerMethod.PATCH, "/actions/" + storage_meta.name + "/{key}"
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
        storage_meta.key_config.from_key_str(key, dumped)
        storage = storage_factory.create(authorization)
        search_filter = precondition.to_search_filter() if precondition else INCLUDE_ALL
        updated = storage.update(dumped, search_filter)
        if updated:
            return marshaller.load(item)

    return update


def action_for_delete(storage_factory: StorageFactoryABC) -> Callable:
    storage_meta = storage_factory.get_storage_meta()

    @action(
        name=f"{storage_meta.name}_delete",
        description=f"Delete an item in {storage_meta.name}",
        triggers=(
            WebTrigger(
                WebTriggerMethod.DELETE, "/actions/" + storage_meta.name + "/{key}"
            ),
        ),
    )
    def delete(key: str, authorization: Optional[Authorization] = None) -> bool:

        storage = storage_factory.create(authorization)
        result = storage.delete(key)
        return result

    return delete


def action_for_search(
    storage_factory: StorageFactoryABC,
    marshaller: MarshallerABC,
    item_type: Type,
    search_filter_type: Type[SearchFilterFactoryABC],
    search_order_type: Type[SearchOrderFactoryABC],
) -> Callable:
    storage_meta = storage_factory.get_storage_meta()
    # noinspection PyTypeChecker
    result_set_type = get_result_set_type(item_type)

    @action(
        name=f"{storage_meta.name}_search",
        description=f"Run a search in {storage_meta.name}",
        triggers=(
            WebTrigger(WebTriggerMethod.GET, f"/actions/{storage_meta.name}-search"),
        ),
    )
    def search(
        search_filter: Optional[search_filter_type] = None,
        search_order: Optional[search_order_type] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
        authorization: Optional[Authorization] = None,
    ) -> result_set_type:
        storage = storage_factory.create(authorization)
        search_filter = (
            search_filter.to_search_filter() if search_filter else INCLUDE_ALL
        )
        search_order = search_order.to_search_order() if search_order else None
        result_set = storage.search(search_filter, search_order, page_key, limit)

        result_set = result_set_type(
            results=[marshaller.load(r) for r in result_set.results],
            next_page_key=result_set.next_page_key,
        )
        return result_set

    return search


def action_for_count(
    storage_factory: StorageFactoryABC,
    search_filter_type: Type[SearchFilterFactoryABC],
) -> Callable:
    storage_meta = storage_factory.get_storage_meta()

    @action(
        name=f"{storage_meta.name}_count",
        description=f"Get a count from {storage_meta.name}",
        triggers=(
            WebTrigger(WebTriggerMethod.GET, f"/actions/{storage_meta.name}-count"),
        ),
    )
    def count(
        search_filter: Optional[search_filter_type] = None,
        authorization: Optional[Authorization] = None,
    ) -> int:
        storage = storage_factory.create(authorization)
        search_filter = (
            search_filter.to_search_filter() if search_filter else INCLUDE_ALL
        )
        result = storage.count(search_filter)
        return result

    return count


def action_for_read_batch(
    storage_factory: StorageFactoryABC, marshaller: MarshallerABC, item_type: Type
) -> Callable:
    storage_meta = storage_factory.get_storage_meta()

    @action(
        name=f"{storage_meta.name}_read_batch",
        description=f"Read a batch of items from {storage_meta.name} given keys",
        triggers=(
            WebTrigger(
                WebTriggerMethod.GET, "/actions/" + storage_meta.name + "-batch"
            ),
        ),
        cache_control=storage_meta.cache_control
    )
    def read_batch(
        keys: List[str], authorization: Optional[Authorization] = None
    ) -> List[Optional[item_type]]:
        storage = storage_factory.create(authorization)
        results = storage.read_batch(keys)
        results = [marshaller.load(r) if r else None for r in results]
        return results

    return read_batch


def action_for_edit_batch(
    storage_factory: StorageFactoryABC,
    create_input_type: Type,
    create_input_type_marshaller: MarshallerABC,
    update_input_type: Type,
    update_input_type_marshaller: MarshallerABC,
) -> Callable:
    storage_meta = storage_factory.get_storage_meta()
    batch_edit_type = batch_edit_dataclass_for(
        storage_meta.name.title() + "BatchEdit", create_input_type, update_input_type
    )
    batch_edit_result_type = batch_edit_result_dataclass_for(batch_edit_type)

    @action(
        name=f"{storage_meta.name}_edit_batch",
        description=f"Perform a batch of edits against {storage_meta.name}",
        triggers=(
            WebTrigger(
                WebTriggerMethod.PATCH, "/actions/" + storage_meta.name + "-batch"
            ),
        ),
    )
    def edit_batch(
        edits: List[batch_edit_type], authorization: Optional[Authorization] = None
    ) -> List[batch_edit_result_type]:
        storage = storage_factory.create(authorization)
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
        results = storage.edit_batch(internal_edits)
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


def get_item_type(storage_meta: StorageMeta):
    name = storage_meta.name.title()
    if hasattr(output, name):
        return getattr(output, name)

    annotations = {}
    params = {
        '__annotations__': annotations
    }
    for f in storage_meta.fields:
        if f.is_readable:
            annotations[f.name] = f.schema.python_type

    for link in storage_meta.links:
        params[link.get_name()] = link.to_action_fn(storage_meta.name)

    type_ = dataclass(type(name, tuple(), params))
    setattr(output, name, type_)
    return type_


def get_result_set_type(item_type: Type) -> Type:
    result_set_name = f"{item_type.__name__}ResultSet"
    result_set_type = getattr(output, result_set_name, None)
    if not result_set_type:
        # noinspection PyTypeChecker
        result_set_type = result_set_dataclass_for(item_type)
        setattr(output, result_set_name, result_set_type)
    return result_set_type
