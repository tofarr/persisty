from dataclasses import dataclass
from typing import Generic, TypeVar, Optional, Union, Iterable, Sized

from boto3.dynamodb.conditions import ConditionBase, Key
from marshy.marshaller_context import MarshallerContext
from marshy.types import ExternalItemType

from lambsync.persistence.dynamo.dynamo_index import DynamoIndex
from lambsync.persistence.dynamo.dynamo_search import DynamoSearch
from lambsync.persistence.dynamo.dynamo_table import DynamoTable

T = TypeVar('T')
TO_DYNAMO_SEARCH = 'to_dynamo_search'


@dataclass
class DynamoSearchFactory(Generic[T]):

    marshaller_context: MarshallerContext

    def create(self,
               table: DynamoTable,
               search_filter: Optional[T],
               projected_attributes: Union[Iterable[str], Sized, None] = None
               ) -> DynamoSearch:
        if hasattr(search_filter, TO_DYNAMO_SEARCH):
            return search_filter.to_dynamo_search(table, projected_attributes)
        search_filter_params = self.marshaller_context.dump(search_filter)
        index = table.choose_index(table, search_filter_params)
        index_name = index.name if index else None
        key = _build_key_from_filter(index, search_filter_params)
        filter_expression = _build_filter_expression(search_filter_params, table.query_attrs)
        return DynamoSearch(table, index_name, key, filter_expression, projected_attributes)


def _build_key_from_filter(dynamo_index: DynamoIndex, search_filter: ExternalItemType) -> Optional[ConditionBase]:
    if not dynamo_index:
        return None
    in_attr = f'{dynamo_index.pk}__in'
    in_partition_key = search_filter.pop(in_attr, [])
    if len(in_partition_key) == 1:
        partition_key_value = in_partition_key[0]
    else:
        partition_key_value = search_filter.pop(f'{dynamo_index.pk}__eq')
    key = Key(dynamo_index.pk).eq(_sanitize(partition_key_value))
    if not dynamo_index.sk:
        return key
    for op in ['eq', 'gt', 'gte', 'lt', 'lte', 'begins_with']:
        attr_name = f'{dynamo_index.sk}__{op}'
        value = _sanitize(search_filter.pop(attr_name, None))
        if value:
            key &= getattr(Key(dynamo_index.sk), op)(value)
            return key

    attr_name = f'{dynamo_index.sk}_in'
    values = _sanitize(search_filter.pop(attr_name, None))
    if not values:
        return key
    for value in values:
        key &= Key(dynamo_index.sk).eq(value)
    return key


def _build_filter_expression(search_filter: ExternalItemType, query_attrs: Iterable[str]) -> Optional[ConditionBase]:
    if not search_filter:
        return None
    condition: Optional[ConditionBase] = None
    for key, value in search_filter.items():
        if not value:
            continue
        found = False
        if key == 'query':
            new_condition = _build_query_condition(value, query_attrs)
            if new_condition:
                condition = condition & new_condition if condition else new_condition
            found = True
        else:
            for op in ['begins_with', 'contains', 'eq', 'ne', 'gt', 'gte', 'lt', 'lte']:
                if key.endswith(f'__{op}'):
                    new_condition = _build_attr_condition(key[:-len(op) - 2], op, value)
                    condition = condition & new_condition if condition else new_condition
                    found = True
                    break
            if key.endswith('__in'):
                found = True
                or_condition = None
                key = key[:-4]
                for v in value:
                    new_condition = Attr(key).eq(v)
                    or_condition = Or(or_condition, new_condition) if or_condition else new_condition
                if or_condition:
                    condition = condition & or_condition if condition else or_condition
        if not found:
            raise StoreError(f'Unknown attribute in filter: {key}')
    return condition


def _build_attr_condition(key: str, op: str, value: Any) -> Optional[ConditionBase]:
    if not value:
        return None
    attr = Attr(key)
    return getattr(attr, op)(_sanitize(value))


def _build_query_condition(value: Any,
                           query_attrs: Iterable[str]
                           ) -> Optional[ConditionBase]:
    if not value:
        return None
    if not query_attrs:
        raise StoreError('No query_attrs defined!')
    condition = None
    for query_attr in query_attrs:
        new_condition = Attr(query_attr).contains(value)
        condition = Or(condition, new_condition) if condition else new_condition
    if not condition:
        raise StoreError(f'Query filter against no values!')
    return condition


def _sanitize(value: Any):
    if isinstance(value, Enum):
        return value.value
    else:
        return value


def _build_key_schema(index: DynamoIndex):
    key_schema = [{
        'AttributeName': index.pk,
        'KeyType': 'HASH'
    }]
    if index.sk:
        key_schema.append({
            'AttributeName': index.sk,
            'KeyType': 'RANGE'
        })
    return key_schema
