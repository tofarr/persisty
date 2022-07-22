from decimal import Decimal
from typing import Optional, Dict, Tuple, List
from dataclasses import dataclass, field

import boto3
from boto3.dynamodb.conditions import Not as DynNot, ConditionBase, Key
from botocore.exceptions import ClientError
from marshy.types import ExternalItemType

from persisty.errors import PersistyError
from persisty.impl.dynamodb.dynamodb_index import DynamodbIndex
from persisty.search_filter.and_filter import And
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.storage.field.field import load_field_values
from persisty.storage.field.field_filter import FieldFilter, FieldFilterOp
from persisty.storage.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_order.search_order import SearchOrder
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.util import filter_none, get_logger
from persisty.util.undefined import UNDEFINED

logger = get_logger(__name__)


def catch_client_error(fn):
    def wrapper(*args, **kwargs):
        try:
            return fn(*args, **kwargs)
        except ClientError as e:
            raise PersistyError(e)
    return wrapper


@dataclass(frozen=True)
class DynamodbTableStorage(StorageABC):
    """Storage backed by a dynamodb table. Does not do single table design or anything of that nature."""

    storage_meta: StorageMeta
    table_name: str
    index: DynamodbIndex
    global_secondary_indexes: Dict[str, DynamodbIndex] = field(default_factory=dict)
    aws_profile_name: Optional[str] = None
    region_name: Optional[str] = None

    def get_storage_meta(self) -> StorageMeta:
        return self.storage_meta

    @catch_client_error
    def create(self, item: ExternalItemType) -> ExternalItemType:
        item = self._dump(item)
        self._dynamodb_table().put_item(
            Item=item,
            ConditionExpression=DynNot(self.index.to_condition_expression(item)),
        )
        return item

    @catch_client_error
    def read(self, key: str) -> Optional[ExternalItemType]:
        table = self._dynamodb_table()
        key_dict = self.storage_meta.key_config.from_key_str(key)
        response = table.get_item(Key=key_dict)
        loaded = self._load(response.get("Item"))
        return loaded

    @catch_client_error
    def read_batch(self, keys: List[str]) -> List[Optional[ExternalItemType]]:
        assert(len(keys) <= self.storage_meta.batch_size)
        key_config = self.storage_meta.key_config
        resource = self._dynamodb_resource()

        kwargs = {
            'RequestItems': {
                self.table_name: {
                    'Keys': [key_config.from_key_str(key) for key in set(keys)]
                }
            }
        }
        results_by_key = {}
        while True:
            response = resource.batch_get_item(**kwargs)
            for item in response['Responses'][self.table_name]:
                key = key_config.to_key_str(item)
                results_by_key[key] = self._load(item)
            unprocessed_keys = response.get('UnprocessedKeys')  # Batch size would have been greater than 16 Mb
            if not unprocessed_keys:
                return [results_by_key.get(key) for key in keys]
            kwargs['RequestItems'] = unprocessed_keys

    @catch_client_error
    def update(
        self, updates: ExternalItemType, search_filter: SearchFilterABC = INCLUDE_ALL
    ) -> Optional[ExternalItemType]:
        if search_filter is not INCLUDE_ALL:
            search_filter.validate_for_fields(self.storage_meta.fields)
            item = self.read(self.storage_meta.key_config.to_key_str(updates))
            if not search_filter.match(item, self.storage_meta.fields):
                return None
        updates = self._dump(updates, True)
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

    @catch_client_error
    def delete(self, key: str) -> bool:
        table = self._dynamodb_table()
        key_dict = self.storage_meta.key_config.from_key_str(key)
        response = table.delete_item(Key=key_dict, ReturnValues="ALL_OLD")
        attributes = response.get("Attributes")
        return bool(attributes)

    @catch_client_error
    def search(
        self,
        search_filter: SearchFilterABC = INCLUDE_ALL,
        search_order: Optional[SearchOrder] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[ExternalItemType]:
        if limit is None:
            limit = self.storage_meta.batch_size
        else:
            assert limit <= self.storage_meta.batch_size
        search_filter.validate_for_fields(self.storage_meta.fields)
        if search_order:
            search_order.validate_for_fields(self.storage_meta.fields)
        if search_filter is EXCLUDE_ALL:
            return ResultSet([])
        index_name, condition, filter_expression, handled = self.to_dynamodb_filter(
            search_filter
        )
        query_args = filter_none(
            {
                "KeyConditionExpression": condition,
                "IndexName": index_name,
                "Select": "SPECIFIC_ATTRIBUTES",
                "ProjectionExpression": ",".join(
                    f.name for f in self.storage_meta.fields if f.is_readable
                ),
                "FilterExpression": filter_expression,
                "Limit": limit,
            }
        )
        if page_key:
            query_args["ExclusiveStartKey"] = self.storage_meta.key_config.from_key_str(page_key)
        table = self._dynamodb_table()
        results = []
        while True:
            if condition:
                response = table.scan(**query_args)
            else:
                response = table.scan(**query_args)
            items = response["Items"]
            if not handled:
                items = [
                    self._load(item)
                    for item in items
                    if search_filter.match(item, self.storage_meta.fields)
                ]
            results.extend(items)
            if len(results) >= limit:
                results = results[:limit]
                next_page_key = self.storage_meta.key_config.to_key_str(results[-1])
                return ResultSet(results, next_page_key)
            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                return ResultSet(results)
            query_args["ExclusiveStartKey"] = last_evaluated_key

    @catch_client_error
    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        search_filter.validate_for_fields(self.storage_meta.fields)
        if search_filter is EXCLUDE_ALL:
            return 0
        index_name, condition, filter_expression, handled = self.to_dynamodb_filter(
            search_filter
        )
        if not handled:
            logger.warning(f"search_filter_not_handled_by_dynamodb:{search_filter}")
            count = sum(1 for _ in self.search_all(search_filter))
            return count
        query_args = filter_none(
            {
                "KeyConditionExpression": condition,
                "IndexName": index_name,
                "Select": "COUNT",
                "FilterExpression": filter_expression,
            }
        )
        table = self._dynamodb_table()
        count = 0
        while True:
            if condition:
                response = table.scan(**query_args)
            else:
                response = table.scan(**query_args)
            count += response["Count"]  # Items
            last_evaluated_key = response.get("LastEvaluatedKey")
            if not last_evaluated_key:
                return count
            query_args["ExclusiveStartKey"] = last_evaluated_key

    def _load(self, item):
        if item is None:
            return None
        item = self._convert_decimals(item)
        return load_field_values(self.storage_meta.fields, item)

    def _convert_decimals(self, item):
        if isinstance(item, dict):
            return {k: self._convert_decimals(v) for k, v in item.items()}
        elif isinstance(item, list):
            return [self._convert_decimals(i) for i in item]
        elif isinstance(item, Decimal):
            int_val = int(item)
            if int_val == item:
                return int_val
            else:
                return float(item)
        else:
            return item

    def _dump(
        self, item: ExternalItemType, is_update: bool = False
    ) -> ExternalItemType:
        result = {}
        for field_ in self.storage_meta.fields:
            value = item.get(field_.name, UNDEFINED)
            if field_.write_transform:
                value = field_.write_transform.transform(value, is_update)
                item[field_.name] = value
            if value is not UNDEFINED:
                result[field_.name] = value
        return result

    def to_dynamodb_filter(
        self, search_filter: SearchFilterABC
    ) -> Tuple[Optional[str], Optional[ConditionBase], Optional[ConditionBase], bool]:
        eq_filters = _get_top_level_eq_filters(search_filter)
        if not eq_filters:
            filter_expression, handled = search_filter.build_filter_expression(
                self.storage_meta.fields
            )
            return None, None, filter_expression, handled
        index_name, index = self.get_index_for_eq_filters(eq_filters)
        if index:
            index_condition, search_filter, index_handled = _separate_index_filters(
                index, eq_filters
            )
            filter_expression, handled = search_filter.build_filter_expression(
                self.storage_meta.fields
            )
            return (
                index_name,
                index_condition,
                filter_expression,
                handled and index_handled,
            )
        filter_expression, handled = search_filter.build_filter_expression(
            self.storage_meta.fields
        )
        return None, None, filter_expression, handled

    def get_index_for_eq_filters(
        self, eq_filters: List[FieldFilter]
    ) -> Tuple[Optional[str], Optional[DynamodbIndex]]:
        attr_names = {f.name for f in eq_filters}
        if self.index.pk in attr_names:
            return None, self.index
        for name, index in self.global_secondary_indexes:
            if index.pk in attr_names:
                return name, index
        return None, None

    def _dynamodb_table(self):
        if hasattr(self, '_table'):
            return self._table
        resource = self._dynamodb_resource()
        table = resource.Table(self.table_name)
        object.__setattr__(self, '_table', table)
        return table

    def _dynamodb_resource(self):
        if hasattr(self, '_resource'):
            return self._resource
        kwargs = filter_none(dict(profile_name=self.aws_profile_name, region_name=self.region_name))
        session = boto3.Session(**kwargs)
        resource = session.resource("dynamodb")
        object.__setattr__(self, '_resource', resource)
        return resource


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


# noinspection PyTypeChecker
def _get_top_level_eq_filters(search_filter: SearchFilterABC) -> List[FieldFilter]:
    if _is_eq_filter(search_filter):
        return [search_filter]
    elif isinstance(search_filter, And):
        return [f for f in search_filter.search_filters if _is_eq_filter(search_filter)]
    return []


def _is_eq_filter(search_filter: SearchFilterABC) -> bool:
    return (
        isinstance(search_filter, FieldFilter) and search_filter.op == FieldFilterOp.eq
    )


def _separate_index_filters(
    index: DynamodbIndex, eq_filters: List[FieldFilter]
) -> Tuple[ConditionBase, SearchFilterABC, bool]:
    index_filters = [f for f in eq_filters if f.name == index.pk]
    condition = Key(index.pk).eq(index_filters[0].value)
    non_index_filters = [f for f in eq_filters if f.name != index.pk]
    non_index_filter = None
    if non_index_filters:
        non_index_filter = And(non_index_filters)
    handled = len(index_filters) == 1
    return condition, non_index_filter, handled
