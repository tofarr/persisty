import dataclasses
from dataclasses import dataclass
from typing import TypeVar, Generic, Iterator, Type, Optional

from persisty.item_comparator import item_comparator_from_value
from persisty.item_comparator.item_comparator_abc import ItemComparatorABC
from persisty.item_filter import attr_filter_from
from persisty.item_filter.and_filter import AndFilter
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.item_filter.query_filter import QueryFilter

T = TypeVar('T')
TO_STORAGE_FILTER = 'to_storage_filter'
SORT = 'sort'
QUERY = 'query'


@dataclass(frozen=True)
class StorageFilter(Generic[T]):
    item_filter: Optional[ItemFilterABC[T]] = None
    item_comparator: Optional[ItemComparatorABC[T]] = None

    def filter_items(self, items: Iterator[T]) -> Iterator[T]:
        if self.item_filter is not None:
            items = (item for item in items if self.item_filter.match(item))
        if self.item_comparator is not None:
            items = iter(sorted(items, key=self.item_comparator.key))
        return items


def storage_filter_from_dataclass(obj, filter_type: Type[T]) -> StorageFilter:
    to_storage_filter = getattr(obj, TO_STORAGE_FILTER, None)
    if to_storage_filter:
        return to_storage_filter(filter_type)
    fields_by_name = {f.name: f for f in dataclasses.fields(obj)}
    sort = getattr(obj, SORT) if SORT in fields_by_name else None
    if sort is not None:
        del fields_by_name[SORT]
        item_comparator = item_comparator_from_value(sort, filter_type)
    else:
        item_comparator = None
    item_filters = []
    query = getattr(obj, QUERY) if QUERY in fields_by_name else None
    if query is not None:
        del fields_by_name[QUERY]
        item_filters.append(QueryFilter[T](query))
    for name in list(fields_by_name.keys()):
        value = getattr(obj, name)
        if value is not None:
            item_filters.append(attr_filter_from(name, value, filter_type))
    if len(item_filters) == 0:
        item_filter = None
    elif len(item_filters) == 1:
        item_filter = item_filters[0]
    else:
        item_filter = AndFilter(item_filters)
    return StorageFilter(item_filter, item_comparator)


def append_to_storage_filter(storage_filter: Optional[StorageFilter], item_filter: ItemFilterABC):
    if storage_filter is None:
        return StorageFilter(item_filter)
    item_filter = storage_filter.item_filter & item_filter if storage_filter.item_filter else item_filter
    return StorageFilter(item_filter, storage_filter.item_comparator)