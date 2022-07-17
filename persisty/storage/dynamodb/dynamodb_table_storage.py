from functools import lru_cache
from typing import Optional, Dict, Tuple, List
from dataclasses import dataclass

import boto3
from boto3.dynamodb.conditions import Not as DynNot, ConditionBase, Key
from marshy.types import ExternalItemType

from persisty.storage.dynamodb.dynamodb_index import DynamodbIndex
from persisty.storage.field.field_filter import FieldFilter, FieldFilterOp
from persisty.storage.result_set import ResultSet
from persisty.storage.search_filter.include_all import INCLUDE_ALL
from persisty.storage.search_filter import SearchFilterABC, And, EXCLUDE_ALL
from persisty.storage.search_order import SearchOrder, NO_ORDER
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.util import filter_none, get_logger
from persisty.util.undefined import UNDEFINED

logger = get_logger(__name__)


@dataclass(frozen=True)
class DynamodbTableStorage(StorageABC):
    """ Storage backed by a dynamodb table. Does not do single table design or anything of that nature. """
    storage_meta: StorageMeta
    table_name: str
    index: DynamodbIndex
    gsis: Dict[str, DynamodbIndex]

    def __post_init__(self):
        # Validate that fields for all indexed values exist
        # Validate that no fields are sortable. (Dynamodb sorting only works in conjunction with a partition key)
        pass

    def create(self, item: ExternalItemType) -> ExternalItemType:
        item = self._dump(item)
        _dynamodb_table(self.table_name).put_item(
            Item=item,
            ConditionExpression=DynNot(self.index.to_condition_expression(item)),
        )
        return item

    def read(self, key: str) -> Optional[ExternalItemType]:
        table = _dynamodb_table(self.table_name)
        key_dict = {}
        self.storage_meta.key_config.set_key(key, key_dict)
        response = table.get_item(Key=key_dict)
        return response.get('Item')

    def update(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        updates = self._dump(item)
        table = _dynamodb_table(self.table_name)
        updates = {**updates}
        updates.pop(self.index.pk)
        if self.index.sk:
            updates.pop(self.index.sk)
        update = _build_update(updates)
        response = table.update_item(
            Key=self.index.to_dict(updates),
            ConditionExpression=self.index.to_condition_expression(item),
            UpdateExpression=update['str'],
            ExpressionAttributeNames=update['names'],
            ExpressionAttributeValues=update['values'],
            ReturnValues='ALL_NEW'
        )
        return response.get('Attributes')

    def delete(self, key: str) -> bool:
        table = _dynamodb_table(self.table_name)
        key_dict = {}
        self.storage_meta.key_config.set_key(key, key_dict)
        response = table.delete_item(
            Key=key_dict,
            ReturnValues='ALL_OLD'
        )
        attributes = response.get('Attributes')
        return bool(attributes)

    def search(self,
               search_filter: SearchFilterABC = INCLUDE_ALL,
               search_order: SearchOrder = NO_ORDER,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[ExternalItemType]:
        assert limit <= self.storage_meta.batch_size
        search_filter.validate_for_fields(self.storage_meta.fields)
        search_order.validate_for_fields(self.storage_meta.fields)
        search_filter.validate_for_fields(self.storage_meta.fields)
        if search_filter is EXCLUDE_ALL:
            return ResultSet([])
        index_name, condition, filter_expression, handled = self.to_dynamodb_filter(search_filter)
        query_args = filter_none({
            'KeyConditionExpression': condition,
            'IndexName': index_name,
            'Select': 'SPECIFIC_ATTRIBUTES',
            'ProjectionExpression': ','.join(f.name for f in self.storage_meta.fields if f.is_readable),
            'FilterExpression': filter_expression,
            'LIMIT': limit
        })
        if page_key:
            exclusive_start_key = query_args['ExclusiveStartKey'] = {}
            self.storage_meta.key_config.set_key(page_key, exclusive_start_key)
        table = _dynamodb_table(self.table_name)
        results = []
        while True:
            if condition:
                response = table.scan(**query_args)
            else:
                response = table.scan(**query_args)
            items = response['Items']
            if not handled:
                items = [item for item in items if search_filter.match(item, self.storage_meta.fields)]
            results.extend(items)
            if len(results) >= limit:
                results = results[:limit]
                next_page_key = self.storage_meta.key_config.get_key(results[-1])
                return ResultSet(results, next_page_key)
            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                return ResultSet(results)
            query_args['ExclusiveStartKey'] = last_evaluated_key

    def count(self, search_filter: SearchFilterABC = INCLUDE_ALL) -> int:
        search_filter.validate_for_fields(self.storage_meta.fields)
        if search_filter is EXCLUDE_ALL:
            return 0
        index_name, condition, filter_expression, handled = self.to_dynamodb_filter(search_filter)
        if not handled:
            logger.warning(f'search_filter_not_handled_by_dynamodb:{search_filter}')
            count = sum(1 for _ in self.search_all(search_filter))
            return count
        query_args = filter_none({
            'KeyConditionExpression': condition,
            'IndexName': index_name,
            'Select': 'COUNT',
            'FilterExpression': filter_expression
        })
        table = _dynamodb_table(self.table_name)
        count = 0
        while True:
            if condition:
                response = table.scan(**query_args)
            else:
                response = table.scan(**query_args)
            count += response['Count']  # Items
            last_evaluated_key = response.get('LastEvaluatedKey')
            if not last_evaluated_key:
                return count
            query_args['ExclusiveStartKey'] = last_evaluated_key

    def _dump(self, item: ExternalItemType, is_update: bool = False) -> ExternalItemType:
        result = {}
        for field_ in self.storage_meta.fields:
            value = item.get(field_.name, UNDEFINED)
            if field_.write_transform:
                value = field_.write_transform.transform(value, is_update)
                item[field_.name] = value
            if value is not UNDEFINED:
                result[field_.name] = value
        return result

    def to_dynamodb_filter(self,
                           search_filter: SearchFilterABC
                           ) -> Tuple[Optional[str], Optional[ConditionBase], Optional[ConditionBase], bool]:
        eq_filters = _get_top_level_eq_filters(search_filter)
        if not eq_filters:
            filter_expression, handled = search_filter.build_filter_expression(self.storage_meta.fields)
            return None, None, filter_expression, handled
        index_name, index = self.get_index_for_eq_filters(eq_filters)
        if index:
            index_condition, search_filter, index_handled = _separate_index_filters(index, eq_filters)
            filter_expression, handled = search_filter.build_filter_expression(self.storage_meta.fields)
            return index_name, index_condition, filter_expression, handled and index_handled
        filter_expression, handled = search_filter.build_filter_expression(self.storage_meta.fields)
        return None, None, filter_expression, handled

    def get_index_for_eq_filters(self, eq_filters: List[FieldFilter]) -> Tuple[Optional[str], Optional[DynamodbIndex]]:
        attr_names = {f.name for f in eq_filters}
        if self.index.pk in attr_names:
            return None, self.index
        for name, index in self.gsis:
            if index.pk in attr_names:
                return name, index
        return None, None


@lru_cache(maxsize=200)
def _dynamodb_table(table_name: str):
    return _dynamodb().Table(table_name)


@lru_cache()
def _dynamodb():
    return boto3.resource('dynamodb')


def _build_update(updates: dict):
    update_str = 'set '
    update_list = []
    names = {}
    values = {}
    for k, v in updates.items():
        update_list.append(f"#n_{k} = :v_{k}")
        names[f"#n_{k}"] = k
        values[f":v_{k}"] = v
    update_str += ', '.join(update_list)
    return {
        'str': update_str,
        'names': names,
        'values': values
    }


# noinspection PyTypeChecker
def _get_top_level_eq_filters(search_filter: SearchFilterABC) -> List[FieldFilter]:
    if _is_eq_filter(search_filter):
        return [search_filter]
    elif isinstance(search_filter, And):
        return [f for f in search_filter if _is_eq_filter(search_filter)]
    return []


def _is_eq_filter(search_filter: SearchFilterABC) -> bool:
    return isinstance(search_filter, FieldFilter) and search_filter.op == FieldFilterOp.eq


def _separate_index_filters(index: DynamodbIndex, eq_filters: List[FieldFilter]) -> Tuple[ConditionBase, SearchFilterABC, bool]:
    index_filters = [f for f in eq_filters if f.name == index.pk]
    condition = Key(index.pk).eq(index_filters[0].value)
    non_index_filters = [f for f in eq_filters if f.name != index.pk]
    non_index_filter = None
    if non_index_filters:
        non_index_filter = And(non_index_filters)
    handled = len(index_filters) == 1
    return condition, non_index_filter, handled
