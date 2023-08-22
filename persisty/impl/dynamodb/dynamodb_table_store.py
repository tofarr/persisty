import os
from copy import deepcopy
from decimal import Decimal
from typing import Optional, Dict, Tuple, List, Set
from dataclasses import dataclass, field

import boto3
import marshy
from boto3.dynamodb.conditions import Not as DynNot, ConditionBase, Key
from botocore.exceptions import ClientError
from marshy.types import ExternalItemType

from persisty.attr.attr import Attr
from persisty.attr.generator.attr_value_generator_abc import AttrValueGeneratorABC
from persisty.errors import PersistyError
from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex
from persisty.search_filter.and_filter import And
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.attr.attr_filter import AttrFilter, AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.batch_edit_result import BatchEditResult
from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC, T
from persisty.store_meta import StoreMeta
from persisty.util import filter_none, get_logger
from persisty.util.undefined import UNDEFINED

logger = get_logger(__name__)


def catch_client_error(fn):
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ClientError as e:
            raise PersistyError(e) from e

    return wrapper


# pylint: disable=R0902
@dataclass(frozen=True)
class DynamodbTableStore(StoreABC[T]):
    """Store backed by a dynamodb table. Does not do single table design or anything of that nature."""

    meta: StoreMeta
    table_name: str
    index: PartitionSortIndex
    global_secondary_indexes: Dict[str, PartitionSortIndex] = field(
        default_factory=dict
    )
    aws_profile_name: Optional[str] = None
    region_name: Optional[str] = field(
        default_factory=lambda: os.environ.get("AWS_REGION")
    )
    decimal_format: str = "%.9f"
    max_local_search_size: int = None

    def __post_init__(self):
        if self.max_local_search_size is None:
            object.__setattr__(self, "max_local_search_size", self.meta.batch_size * 5)

    def get_meta(self) -> StoreMeta:
        return self.meta

    @catch_client_error
    def create(self, item: T) -> T:
        item = self._dump_create(item)
        self._dynamodb_table().put_item(
            Item=item,
            ConditionExpression=DynNot(self.index.to_condition_expression(item)),
        )
        loaded = self._load(item)
        return loaded

    @catch_client_error
    def read(self, key: str) -> Optional[T]:
        table = self._dynamodb_table()
        if not isinstance(key, str):
            key = str(key)
        key_dict = self.meta.key_config.to_key_dict(key)
        response = table.get_item(Key=key_dict)
        loaded = self._load(response.get("Item"))
        return loaded

    @catch_client_error
    def read_batch(self, keys: List[str]) -> List[Optional[T]]:
        assert len(keys) <= self.meta.batch_size
        key_config = self.meta.key_config
        resource = self._dynamodb_resource()

        kwargs = {
            "RequestItems": {
                self.table_name: {
                    "Keys": [key_config.to_key_dict(key) for key in set(keys)]
                }
            }
        }
        results_by_key = {}
        response = resource.batch_get_item(**kwargs)
        for item in response["Responses"][self.table_name]:
            loaded = self._load(item)
            key = key_config.to_key_str(loaded)
            results_by_key[key] = loaded
        assert not response.get(
            "UnprocessedKeys"
        )  # Batch size would have been greater than 16 Mb
        return [results_by_key.get(key) for key in keys]

    @catch_client_error
    def update(
        self, updates: T, precondition: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[T]:
        if precondition is not INCLUDE_ALL:
            search_filter = precondition.lock_attrs(self.meta.attrs)
            item = self.read(self.meta.key_config.to_key_str(updates))
            if not search_filter.match(item, self.meta.attrs):
                return None
        updates = self._dump_update(updates)
        key_dict = self.index.to_dict(updates)
        table = self._dynamodb_table()
        updates = {**updates}
        updates.pop(self.index.pk)
        if self.index.sk:
            updates.pop(self.index.sk)
        update = _build_update(updates)
        response = table.update_item(
            Key=key_dict,
            ConditionExpression=self.index.to_condition_expression(key_dict),
            UpdateExpression=update["str"],
            ExpressionAttributeNames=update["names"],
            ExpressionAttributeValues=update["values"],
            ReturnValues="ALL_NEW",
        )
        loaded = self._load(response.get("Attributes"))
        return loaded

    def _update(
        self,
        key: str,
        item: T,
        updates: T,
        search_filter: SearchFilterABC = INCLUDE_ALL,
    ) -> Optional[T]:
        return self.update(updates, search_filter)

    @catch_client_error
    def delete(self, key: str) -> bool:
        table = self._dynamodb_table()
        key_dict = self.meta.key_config.to_key_dict(key)
        response = table.delete_item(Key=key_dict, ReturnValues="ALL_OLD")
        attributes = response.get("Attributes")
        return bool(attributes)

    def _delete(self, key: str, item: T) -> bool:
        return self.delete(key)

    @catch_client_error
    def search(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[T]:
        if limit is None:
            limit = self.meta.batch_size
        assert limit <= self.meta.batch_size
        search_filter = search_filter.lock_attrs(self.meta.attrs)
        if search_order:
            search_order.validate_for_attrs(self.meta.attrs)
        if search_filter is EXCLUDE_ALL:
            return ResultSet([])
        index_name, index = self._get_index_for_search(search_filter, search_order)
        key_filter, other_filter = _separate_index_from_filter(index, search_filter)
        if other_filter:
            (
                filter_expression,
                search_filter_handled_natively,
            ) = other_filter.build_filter_expression(self.meta.attrs)
        else:
            filter_expression = None
            search_filter_handled_natively = True
        query_args = filter_none(
            {
                "KeyConditionExpression": self._to_key_condition_expression(key_filter),
                "IndexName": index_name,
                "Select": "SPECIFIC_ATTRIBUTES",
                "ProjectionExpression": ",".join(
                    a.name for a in self.meta.attrs if a.readable
                ),
                "FilterExpression": filter_expression,
                "ScanIndexForward": _get_scan_index_forward(index, search_order),
            }
        )
        search_order_handled_natively = _is_search_order_handled_natively(
            index, search_order
        )
        if search_order_handled_natively:
            return self._search_native_order(
                query_args,
                index,
                other_filter,
                search_filter_handled_natively,
                page_key,
                limit,
            )
        return self._search_local_order(
            query_args,
            index,
            other_filter,
            search_filter_handled_natively,
            search_order,
            page_key,
            limit,
        )

    # pylint: disable=R0913
    def _search_native_order(
        self,
        query_args: Dict,
        index: Optional[PartitionSortIndex],
        search_filter: SearchFilterABC,
        search_filter_handled_natively: bool,
        page_key: Optional[str],
        limit: int,
    ) -> ResultSet[T]:
        if page_key:
            query_args["ExclusiveStartKey"] = self.meta.key_config.to_key_dict(page_key)
        table = self._dynamodb_table()
        results = []
        while True:
            response = _get_search_response(table, index, query_args)
            items = self._load_items(
                response, search_filter, search_filter_handled_natively
            )
            results.extend(items)
            if len(results) >= limit:
                results = results[:limit]
                next_page_key = self.meta.key_config.to_key_str(results[-1])
                assert next_page_key != page_key  # Paranoid prevent infinite loop!
                return ResultSet(results, next_page_key)
            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                return ResultSet(results)
            query_args["ExclusiveStartKey"] = last_evaluated_key

    # pylint: disable=R0914
    def _search_local_order(
        self,
        query_args: Dict,
        index: Optional[PartitionSortIndex],
        search_filter: SearchFilterABC,
        search_filter_handled_natively: bool,
        search_order: SearchOrder,
        page_key: Optional[str],
        limit: int,
    ) -> ResultSet[T]:
        table = self._dynamodb_table()
        results = []
        while True:
            response = _get_search_response(table, index, query_args)
            items = self._load_items(
                response, search_filter, search_filter_handled_natively
            )
            results.extend(items)
            if len(items) > self.max_local_search_size:
                raise PersistyError("sort_failed")
            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                results = list(search_order.sort(results))
                key_config = self.meta.key_config
                offset = 0
                if page_key:
                    offset = next(
                        i + 1
                        for i, result in enumerate(results)
                        if key_config.to_key_str(result) == page_key
                    )
                next_page_key = None
                if len(results) > offset + limit:
                    next_page_key = key_config.to_key_str(results[offset + limit - 1])
                results = results[offset : (offset + limit)]
                return ResultSet(results, next_page_key)
            query_args["ExclusiveStartKey"] = last_evaluated_key

    @catch_client_error
    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        search_filter = search_filter.lock_attrs(self.meta.attrs)
        if search_filter is EXCLUDE_ALL:
            return 0
        index_name, index = self._get_index_for_search(search_filter, None)
        key_filter, other_filter = _separate_index_from_filter(index, search_filter)
        if other_filter:
            (
                filter_expression,
                search_filter_handled_natively,
            ) = other_filter.build_filter_expression(self.meta.attrs)
        else:
            filter_expression = None
            search_filter_handled_natively = True
        if not search_filter_handled_natively:
            result = sum(1 for _ in self.search_all(search_filter))
            return result
        kwargs = filter_none(
            {
                "KeyConditionExpression": self._to_key_condition_expression(key_filter),
                "IndexName": index_name,
                "Select": "COUNT",
                "FilterExpression": filter_expression,
            }
        )
        table = self._dynamodb_table()
        count = 0
        while True:
            if index:
                response = table.query(**kwargs)
            else:
                response = table.scan(**kwargs)
            count += response["Count"]  # Items
            last_evaluated_key = response.get("LastEvaluatedKey")
            kwargs["ExclusiveStartKey"] = last_evaluated_key
            if not last_evaluated_key:
                return count

    def _to_key_condition_expression(self, key_filter: Optional[AttrFilter]):
        if not key_filter:
            return
        attr = next(a for a in self.meta.attrs if a.name == key_filter.name)
        marshy.dump(key_filter.value, attr.schema.python_type)
        value = marshy.dump(key_filter.value, attr.schema.python_type)
        return Key(key_filter.name).eq(value)

    def _edit_batch(
        self, edits: List[BatchEdit], items_by_key: Dict[str, T]
    ) -> List[BatchEditResult]:
        assert len(edits) <= self.meta.batch_size
        results = []
        key_config = self.meta.key_config
        table = self._dynamodb_table()
        with table.batch_writer() as batch:
            for edit in edits:
                if edit.create_item:
                    item = self._dump_create(edit.create_item)
                    batch.put_item(Item=item)
                    results.append(BatchEditResult(edit, True))
                elif edit.update_item:
                    updates = edit.update_item
                    key = key_config.to_key_str(updates)
                    item = items_by_key[key]
                    to_put = {}
                    for attr in self.meta.attrs:
                        value = UNDEFINED
                        if attr.update_generator:
                            if attr.updatable:
                                value = attr.update_generator.transform(
                                    getattr(updates, attr.name), updates
                                )
                            else:
                                value = attr.update_generator.transform(
                                    UNDEFINED, updates
                                )
                        elif attr.updatable:
                            value = getattr(updates, attr.name)
                        if value is UNDEFINED:
                            value = getattr(item, attr.name)
                        else:
                            setattr(item, attr.name, value)
                        value = marshy.dump(value, attr.schema.python_type)
                        to_put[attr.name] = self._convert_to_decimals(value)
                    to_put = self._convert_to_decimals(to_put)
                    batch.put_item(Item=to_put)
                    edit.update_item = deepcopy(item)  # In case of multi put
                    results.append(BatchEditResult(edit, True))
                else:
                    key = key_config.to_key_dict(edit.delete_key)
                    batch.delete_item(Key=key)
                    results.append(BatchEditResult(edit, True))
        return results

    def _load(self, item) -> T:
        if item is None:
            return None
        item = self._convert_from_decimals(item)
        kwargs = {}
        for attr in self.meta.attrs:
            value = item.get(attr.name, UNDEFINED)
            if attr.readable and value is not UNDEFINED:
                # noinspection PyTypeChecker
                kwargs[attr.name] = marshy.load(attr.schema.python_type, value)
        result = self.meta.get_read_dataclass()(**kwargs)
        return result

    def _convert_from_decimals(self, item):
        if isinstance(item, dict):
            return {k: self._convert_from_decimals(v) for k, v in item.items()}
        if isinstance(item, list):
            return [self._convert_from_decimals(i) for i in item]
        if isinstance(item, Decimal):
            int_val = int(item)
            if int_val == item:
                return int_val
            return float(item)
        return item

    def _dump_create(self, to_create: T):
        result = {}
        for attr in self.meta.attrs:
            self._dump_attr(
                to_create, attr, attr.creatable, attr.create_generator, result
            )
        return result

    def _dump_update(self, to_update: T):
        result = {}
        for attr in self.meta.attrs:
            self._dump_attr(
                to_update, attr, attr.updatable, attr.update_generator, result
            )
        return result

    def _dump_attr(
        self,
        item: T,
        attr: Attr,
        accepts_input: bool,
        generator: Optional[AttrValueGeneratorABC],
        target: ExternalItemType,
    ):
        value = UNDEFINED
        if accepts_input:
            value = getattr(item, attr.name, UNDEFINED)
        if generator:
            value = generator.transform(value, item)
        if value is not UNDEFINED:
            value = marshy.dump(value, attr.schema.python_type)
            target[attr.name] = self._convert_to_decimals(value)

    def _convert_to_decimals(self, item):
        if isinstance(item, dict):
            return {k: self._convert_to_decimals(v) for k, v in item.items()}
        if isinstance(item, list):
            return [self._convert_to_decimals(i) for i in item]
        if isinstance(item, float):
            int_val = int(item)
            if int_val == item:
                return int_val
            return Decimal(self.decimal_format % item)
        return item

    def _get_index_for_search(
        self,
        search_filter: SearchFilterABC,
        search_order: Optional[SearchOrder],
    ) -> Tuple[Optional[str], Optional[PartitionSortIndex]]:
        eq_attr_names = _get_top_level_eq_attrs(search_filter)
        sort_attr_names = (
            {s.attr for s in search_order.orders} if search_order else set()
        )
        name = None
        score = _get_score_for_index(self.index, eq_attr_names, sort_attr_names)
        index = self.index if score else None
        for gsi_name, gsi in self.global_secondary_indexes.items():
            gsi_score = _get_score_for_index(gsi, eq_attr_names, sort_attr_names)
            if gsi_score > score:
                name = gsi_name
                score = gsi_score
                index = gsi
        if not score:
            index = None
        return name, index

    def _dynamodb_table(self):
        if hasattr(self, "_table"):
            return self._table
        resource = self._dynamodb_resource()
        table = resource.Table(self.table_name)
        object.__setattr__(self, "_table", table)
        return table

    def _dynamodb_resource(self):
        if hasattr(self, "_resource"):
            return self._resource
        kwargs = filter_none(
            {"profile_name": self.aws_profile_name, "region_name": self.region_name}
        )
        session = boto3.Session(**kwargs)
        resource = session.resource("dynamodb")
        object.__setattr__(self, "_resource", resource)
        return resource

    def _load_items(self, response, search_filter, search_filter_handled_natively):
        items = [self._load(item) for item in response["Items"]]
        if not search_filter_handled_natively:
            items = [
                item for item in items if search_filter.match(item, self.meta.attrs)
            ]
        return items


def _build_update(updates: dict):
    update_str = "set "
    update_list = []
    names = {}
    values = {}
    for k, v in updates.items():
        update_list.append(f"#n_{k} = :v_{k}")
        names[f"#n_{k}"] = k
        values[f":v_{k}"] = v
    update_str += ", ".join(update_list)
    return {"str": update_str, "names": names, "values": values}


def _get_top_level_eq_attrs(search_filter: SearchFilterABC) -> List[str]:
    if isinstance(search_filter, AttrFilter) and search_filter.op == AttrFilterOp.eq:
        return [search_filter.name]
    if isinstance(search_filter, And):
        return [
            f.name
            for f in search_filter.search_filters
            if isinstance(f, AttrFilter) and f.op == AttrFilterOp.eq
        ]
    return []


def _separate_index_filters(
    index: PartitionSortIndex, eq_filters: List[AttrFilter]
) -> Tuple[ConditionBase, SearchFilterABC, bool]:
    index_filters = [f for f in eq_filters if f.name == index.pk]
    value = index_filters[0].value
    if value.__class__ not in (str, int, float, bool):
        value = marshy.dump(value)
    condition = Key(index.pk).eq(value)
    non_index_filters = tuple(f for f in eq_filters if f.name != index.pk)
    non_index_filter = None
    if non_index_filters:
        non_index_filter = And(non_index_filters)
    handled = len(index_filters) == 1
    return condition, non_index_filter, handled


def _get_score_for_index(
    index: PartitionSortIndex, eq_attrs: Set[str], sort_attrs: Set[str]
):
    if index.pk not in eq_attrs:
        return 0
    if index.sk in sort_attrs:
        return 20
    return 10


def _separate_index_from_filter(
    index: Optional[PartitionSortIndex], search_filter: SearchFilterABC
) -> Tuple[Optional[AttrFilter], Optional[SearchFilterABC]]:
    if not index:
        return None, search_filter
    if isinstance(search_filter, AttrFilter):
        return search_filter, None
    index_filter = None
    filters = []
    # noinspection PyUnresolvedReferences
    for f in search_filter.search_filters:
        if f.name == index.pk:
            index_filter = f
        else:
            filters.append(f)
    filter_expression = And(filters) if filters else None
    return index_filter, filter_expression


def _get_scan_index_forward(
    index: Optional[PartitionSortIndex], search_order: Optional[SearchOrder]
) -> Optional[bool]:
    if search_order and _is_search_order_handled_natively(index, search_order):
        return not search_order.orders[0].desc


def _is_search_order_handled_natively(
    index: Optional[PartitionSortIndex], search_order: Optional[SearchOrder]
) -> bool:
    if not search_order:
        return True
    if len(search_order.orders) > 1 or not index:
        return False
    return search_order.orders[0].attr == index.sk


def _get_search_response(table, index, query_args):
    if index:
        response = table.query(**query_args)
    else:
        response = table.scan(**query_args)
    return response
