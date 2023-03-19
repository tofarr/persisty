"""
Namespace for servey actions for a dynamic store. All items are encoded as json strings, since this is the only way
that appsync allows for unstructured data.

"""
import dataclasses
import json
from typing import List, Optional, Dict

import marshy
from servey.action.action import action, get_action
from servey.security.authorization import Authorization
from servey.trigger.web_trigger import WEB_POST, WEB_GET

from persisty.batch_edit import BatchEdit, batch_edit_dataclass_for
from persisty.batch_edit_result import BatchEditResult
from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_order.search_order import SearchOrder
from persisty.store_meta import StoreMeta

_DYNAMIC_STORE = None


def _get_dynamic_store():
    global _DYNAMIC_STORE
    dynamic_store = _DYNAMIC_STORE
    if dynamic_store:
        from persisty_dynamic.dynamic_store_abc import get_dynamic_store

        dynamic_store = _DYNAMIC_STORE = get_dynamic_store()
    return dynamic_store


def add_actions_for_dynamic_stores(target: Dict):
    for value in globals().values():
        action_ = get_action(value)
        if action_:
            target[action_.name] = action_.fn


@action(triggers=WEB_POST)
def dynamic_create(
    store_name: str, item: str, authorization: Optional[Authorization] = None
) -> str:
    dynamic_store = _get_dynamic_store()
    store = dynamic_store.get_store(store_name, authorization)
    meta = store.get_meta()
    stored_dataclass = meta.get_stored_dataclass()
    item_json = json.loads(item)
    item = marshy.load(stored_dataclass, item_json)
    created = store.create(item)
    created_json = marshy.dump(created)
    created_str = json.dumps(created_json)
    return created_str


@action(triggers=WEB_GET)
def dynamic_read(
    store_name: str, key: str, authorization: Optional[Authorization] = None
) -> str:
    dynamic_store = _get_dynamic_store()
    store = dynamic_store.get_store(store_name, authorization)
    read_ = store.read(key)
    if read_:
        read_json = marshy.dump(read_)
        read_str = json.dumps(read_json)
        return read_str


@action(triggers=WEB_POST)
def dynamic_update(
    store_name: str, item: str, authorization: Optional[Authorization] = None
) -> str:
    dynamic_store = _get_dynamic_store()
    store = dynamic_store.get_store(store_name, authorization)
    meta = store.get_meta()
    stored_dataclass = meta.get_stored_dataclass()
    item_json = json.loads(item)
    item = marshy.load(stored_dataclass, item_json)
    updated = store.update(item)
    updated_json = marshy.dump(updated)
    updated_str = json.dumps(updated_json)
    return updated_str


@action(triggers=WEB_POST)
def dynamic_delete(
    store_name: str, key: str, authorization: Optional[Authorization] = None
) -> bool:
    dynamic_store = _get_dynamic_store()
    store = dynamic_store.get_store(store_name, authorization)
    result = store.delete(key)
    return result


def _search_filter(meta: StoreMeta, search_filter: str):
    if search_filter:
        search_filter_factory_type = meta.get_search_filter_factory_dataclass()
        search_filter_json = json.loads(search_filter)
        search_filter_factory = marshy.load(
            search_filter_factory_type, search_filter_json
        )
        search_filter = search_filter_factory.to_search_filter()
    else:
        search_filter = INCLUDE_ALL
    return search_filter


@action(triggers=WEB_GET)
def dynamic_search(
    store_name: str,
    search_filter: str = None,
    search_order: Optional[SearchOrder[str]] = None,
    page_key: Optional[str] = None,
    limit: Optional[int] = None,
    authorization: Optional[Authorization] = None,
) -> ResultSet[str]:
    dynamic_store = _get_dynamic_store()
    store = dynamic_store.get_store(store_name, authorization)
    meta = store.get_meta()
    search_filter = _search_filter(meta, search_filter)
    if search_order:
        search_order_type = meta.get_sort_order_factory_dataclass()
        # noinspection PyArgumentList,PyDataclass
        search_order = search_order_type(
            **{
                f.name: getattr(search_order, f.name)
                for f in dataclasses.fields(search_order_type)
            }
        )
        search_order = search_order.to_search_order()
    result_set = store.search(search_filter, search_order, page_key, limit)
    result_set.results = [json.dumps(marshy.dump(r)) for r in result_set.results]
    return result_set


@action(triggers=WEB_GET)
def dynamic_count(
    store_name: str,
    search_filter: str = None,
    authorization: Optional[Authorization] = None,
) -> int:
    dynamic_store = _get_dynamic_store()
    store = dynamic_store.get_store(store_name, authorization)
    meta = store.get_meta()
    search_filter = _search_filter(meta, search_filter)
    result = store.count(search_filter)
    return result


@action(triggers=WEB_GET)
def dynamic_read_batch(
    store_name: str, keys: List[str], authorization: Optional[Authorization] = None
) -> List[Optional[str]]:
    dynamic_store = _get_dynamic_store()
    store = dynamic_store.get_store(store_name, authorization)
    results = [
        json.dumps(marshy.dump(item)) if item else None
        for item in store.read_batch(keys)
    ]
    return results


@action(triggers=WEB_POST)
def dynamic_edit_batch(
    store_name: str,
    edits: List[BatchEdit[str, str]],
    authorization: Optional[Authorization] = None,
) -> List[BatchEditResult[str, str]]:
    dynamic_store = _get_dynamic_store()
    store = dynamic_store.get_store(store_name, authorization)
    meta = store.get_meta()
    batch_edits = []
    for edit in edits:
        if edit.create_item:
            create_item = marshy.load(
                meta.get_create_dataclass(), json.loads(edit.create_item)
            )
            batch_edits.append(BatchEdit(create_item=create_item))
        elif edit.update_item:
            update_item = marshy.load(
                meta.get_update_dataclass(), json.loads(edit.update_item)
            )
            batch_edits.append(BatchEdit(update_item=update_item))
        else:
            batch_edits.append(edit)
    edit_results = store.edit_batch(batch_edits)
    batch_edit_type = batch_edit_dataclass_for(
        meta.name.title() + "BatchEdit",
        meta.get_create_dataclass(),
        meta.get_update_dataclass(),
    )
    edit_results = [
        BatchEditResult(
            edit=json.dumps(marshy.dump(r.edit, batch_edit_type)),
            success=r.success,
            code=r.code,
            details=r.details,
        )
        for r in edit_results
    ]
    return edit_results
