import dataclasses
import inspect
from typing import Type, Optional, List

from servey.action.action import Action, action, get_action
from servey.action.batch_invoker import BatchInvoker
from servey.security.authorization import Authorization
from servey.trigger.web_trigger import WebTrigger, WebTriggerMethod

from persisty.batch_edit import batch_edit_dataclass_for
from persisty.batch_edit_result import batch_edit_result_dataclass_for
from persisty.link.link_abc import LinkABC
from persisty.result import to_result
from persisty.result_set import result_set_dataclass_for
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_factory import SearchFilterFactoryABC
from persisty.search_order.search_order_factory import SearchOrderFactoryABC
from persisty.servey import generated
from persisty.store_meta import get_meta, StoreMeta


def action_for_create(
    store_meta: StoreMeta,
    result_type: Type,
    create_input_type: Type,
) -> Action:

    @action(
        name=f"{store_meta.name}_create",
        description=f"Create and return an item in {store_meta.name}",
        triggers=WebTrigger(
            WebTriggerMethod.POST, "/actions/" + store_meta.name.replace("_", "-")
        ),
    )
    def create(
        item: create_input_type, authorization: Optional[Authorization] = None
    ) -> Optional[result_type]:
        secured_store = store_meta.create_secured_store(authorization)
        created = secured_store.create(item)
        result = to_result(created, secured_store.get_meta())
        return result

    return get_action(create)


def action_for_read(store_meta: StoreMeta, result_type: Type) -> Action:

    @action(
        name=f"{store_meta.name}_read",
        description=f"Read an item from {store_meta.name} given a key",
        triggers=(
            WebTrigger(
                WebTriggerMethod.GET,
                "/actions/" + store_meta.name.replace("_", "-") + "/{key}",
            ),
        ),
        cache_control=store_meta.cache_control,
    )
    def read(
        key: str, authorization: Optional[Authorization] = None
    ) -> Optional[result_type]:
        secured_store = store_meta.create_secured_store(authorization)
        item = secured_store.read(key)
        result = to_result(item, secured_store.get_meta())
        return result

    return get_action(read)


def action_for_update(
    store_meta: StoreMeta,
    result_type: Type,
    update_input_type: Type,
    search_filter_type: Type[SearchFilterFactoryABC],
) -> Action:

    @action(
        name=f"{store_meta.name}_update",
        description=f"Update and return an item in {store_meta.name}",
        triggers=(
            WebTrigger(
                WebTriggerMethod.PATCH,
                "/actions/" + store_meta.name.replace("_", "-") + "/{key}",
            ),
        ),
    )
    def update(
        key: str,
        item: update_input_type,
        precondition: Optional[search_filter_type] = None,
        authorization: Optional[Authorization] = None,
    ) -> Optional[result_type]:
        store_meta.key_config.from_key_str(key, item)
        secured_store = store_meta.create_secured_store(authorization)
        search_filter = _create_search_filter(search_filter_type, precondition)
        updated = secured_store.update(item, search_filter)
        result = to_result(updated, secured_store.get_meta())
        return result

    return get_action(update)


def action_for_delete(store_meta: StoreMeta) -> Action:

    @action(
        name=f"{store_meta.name}_delete",
        description=f"Delete an item in {store_meta.name}",
        triggers=(
            WebTrigger(
                WebTriggerMethod.DELETE,
                "/actions/" + store_meta.name.replace("_", "-") + "/{key}",
            ),
        ),
    )
    def delete(key: str, authorization: Optional[Authorization] = None) -> bool:
        secured_store = store_meta.create_secured_store(authorization)
        result = secured_store.delete(key)
        return result

    return get_action(delete)


def action_for_search(
    store_meta: StoreMeta,
    result_type: Type,
    search_filter_type: Optional[Type[SearchFilterFactoryABC]],
    search_order_type: Type[SearchOrderFactoryABC],
) -> Action:

    item_name = store_meta.name.title().replace("_", "")
    result_set_name = f"{item_name}ResultSet"
    # noinspection PyTypeChecker
    result_set_type = result_set_dataclass_for(result_type, result_set_name)
    setattr(generated, result_set_type.__name__, result_set_type)

    @action(
        name=f"{store_meta.name}_search",
        description=f"Run a search in {store_meta.name}",
        triggers=(
            WebTrigger(
                WebTriggerMethod.GET,
                f"/actions/{store_meta.name.replace('_', '-')}-search",
            ),
        ),
    )
    def search(
        search_filter: Optional[search_filter_type] = None,
        search_order: Optional[search_order_type] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
        authorization: Optional[Authorization] = None,
    ) -> result_set_type:
        secured_store = store_meta.create_secured_store(authorization)
        search_filter = _create_search_filter(search_filter_type, search_filter)
        if search_order:
            # noinspection PyArgumentList,PyDataclass
            search_order = search_order_type(
                **{
                    f.name: getattr(search_order, f.name)
                    for f in dataclasses.fields(search_order_type)
                }
            )
            search_order = search_order.to_search_order()
        result_set = secured_store.search(search_filter, search_order, page_key, limit)

        # noinspection PyArgumentList
        result_set = result_set_type(
            results=[
                to_result(item, secured_store.get_meta()) for item in result_set.results
            ],
            next_page_key=result_set.next_page_key,
        )
        return result_set

    if search_order_type is None:
        sig = inspect.signature(search)
        sig = sig.replace(
            parameters=[p for p in sig.parameters.values() if p.name != "search_order"]
        )
        search.__signature__ = sig

    search = action(
        fn=search,
        name=f"{store_meta.name}_search",
        description=f"Run a search in {store_meta.name}",
        triggers=(
            WebTrigger(
                WebTriggerMethod.GET,
                f"/actions/{store_meta.name.replace('_', '-')}-search",
            ),
        ),
    )

    return get_action(search)


def action_for_count(
    store_meta: StoreMeta,
    search_filter_type: Type[SearchFilterFactoryABC],
) -> Action:

    @action(
        name=f"{store_meta.name}_count",
        description=f"Get a count from {store_meta.name}",
        triggers=(
            WebTrigger(
                WebTriggerMethod.GET,
                f"/actions/{store_meta.name.replace('_', '-')}-count",
            ),
        ),
    )
    def count(
        search_filter: Optional[search_filter_type] = None,
        authorization: Optional[Authorization] = None,
    ) -> int:
        secured_store = store_meta.create_secured_store(authorization)
        search_filter = _create_search_filter(search_filter_type, search_filter)
        result = secured_store.count(search_filter)
        return result

    return get_action(count)


def action_for_read_batch(store_meta: StoreMeta, result_type: Type) -> Action:

    @action(
        name=f"{store_meta.name}_read_batch",
        description=f"Read a batch of items from {store_meta.name} given keys",
        triggers=WebTrigger(
            WebTriggerMethod.GET,
            "/actions/" + store_meta.name.replace("_", "-") + "-batch",
        ),
        cache_control=store_meta.cache_control,
    )
    def read_batch(
        keys: List[str], authorization: Optional[Authorization] = None
    ) -> List[Optional[result_type]]:
        secured_store = store_meta.create_secured_store(authorization)
        items = secured_store.read_batch(keys)
        results = [to_result(item, secured_store.get_meta()) for item in items]
        return results

    return get_action(read_batch)


def action_for_edit_batch(
    store_meta: StoreMeta,
    create_input_type: Type,
    update_input_type: Type,
) -> Action:
    batch_edit_type = batch_edit_dataclass_for(
        store_meta.name.title() + "BatchEdit", create_input_type, update_input_type
    )
    batch_edit_result_type = batch_edit_result_dataclass_for(batch_edit_type)

    @action(
        name=f"{store_meta.name}_edit_batch",
        description=f"Perform a batch of edits against {store_meta.name}",
        triggers=WebTrigger(
            WebTriggerMethod.PATCH,
            "/actions/" + store_meta.name.replace("_", "-") + "-batch",
        ),
    )
    def edit_batch(
        edits: List[batch_edit_type], authorization: Optional[Authorization] = None
    ) -> List[batch_edit_result_type]:
        secured_store = store_meta.create_secured_store(authorization)
        results = secured_store.edit_batch(edits)
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

    return get_action(edit_batch)


def wrap_links_in_actions(read_type: Type):
    meta = get_meta(read_type)
    overrides = {}
    for k, v in read_type.__dict__.items():
        if isinstance(v, LinkABC):
            overrides[k] = _to_action_fn(meta, v)
    if overrides:
        overrides["__doc__"] = read_type.__dict__.get("__doc__")
        # noinspection PyTypeChecker
        read_type = dataclasses.dataclass(
            type(read_type.__name__, (read_type,), overrides)
        )
    setattr(generated, read_type.__name__, read_type)
    return read_type


def _to_action_fn(meta: StoreMeta, link: LinkABC):
    return_type = link.get_linked_type("persisty.servey.generated")

    # pylint: disable=C2801
    def wrapper(self, authorization: Optional[Authorization] = None) -> return_type:
        fn = link
        if hasattr(link, "__get__"):
            fn = link.__get__(self, self.__class__)
        # noinspection PyCallingNonCallable
        result = fn(authorization)
        return result

    batch_invoker = None
    if hasattr(link, "batch_call"):
        if hasattr(link, "arg_extractor"):
            arg_extractor = link.arg_extractor
        else:

            def arg_extractor(c):
                return [meta.key_config.to_key_str(c)]

        batch_invoker = BatchInvoker(
            fn=getattr(link, "batch_call"),
            arg_extractor=arg_extractor,
            max_batch_size=meta.batch_size,
        )

    wrapped = action(
        wrapper, name=meta.name + "_" + link.get_name(), batch_invoker=batch_invoker
    )

    return wrapped


def _create_search_filter(search_filter_factory_type, search_filter_input):
    # Since this may be from graphql, it may not actually be the class we expect, so we
    # replace the instance
    if not search_filter_input:
        return INCLUDE_ALL
    kwargs = {
        f.name: getattr(search_filter_input, f.name)
        for f in dataclasses.fields(search_filter_input)
    }
    search_filter_factory = search_filter_factory_type(**kwargs)
    search_filter = search_filter_factory.to_search_filter()
    return search_filter
