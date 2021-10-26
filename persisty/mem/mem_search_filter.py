from dataclasses import dataclass
from typing import Optional, Callable, List, Any, Generic, TypeVar, Iterator

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

ItemFilter = Callable[[ExternalItemType], Optional[ExternalItemType]]
T = TypeVar('T')


@dataclass(frozen=True)
class MemSearchFilter(Generic[T]):
    marshaller: MarshallerABC[T]

    def create_item_filter(self, search_filter: T) -> Optional[ItemFilter]:
        if search_filter is None:
            return None
        search_filter_params = self.marshaller.dump(search_filter)
        filters = []
        append_query_filter(search_filter_params.get('query'), filters)
        append_fn_filters(search_filter_params, filters)
        append_str_filters(search_filter_params, filters)
        append_one_of_filters(search_filter_params, filters)
        return composite_filter(filters)

    def filter_results(self, search_filter: T, items: Iterator[ExternalItemType]) -> Iterator[ExternalItemType]:
        item_filter = self.create_item_filter(search_filter)
        if item_filter:
            items = (item for item in items if item_filter(item))
        order = getattr(search_filter, 'sort', None)
        if order:
            items = sorted(items, key=lambda item: getattr(item, order))
        return items


def append_query_filter(query: Optional[str], target: List[ItemFilter]):
    if not query:
        return

    def filter_by_query(item: ExternalItemType) -> Optional[ExternalItemType]:
        for k in item.keys():
            if not k.endswith('_id'):
                v = item[k]
                if query in str(v):
                    return item

    target.append(filter_by_query)


def append_fn_filters(search_filter_params: ExternalItemType, target: List[ItemFilter]):
    for fn in [eq, gt, gte, lt, lte, ne]:
        fn_name = f'__{fn.__name__}'
        for key, filter_value in search_filter_params.items():
            if key.endswith(fn_name) and filter_value is not None:
                attr_name = key[:-len(fn_name)]
                target.append(AttrItemFilter(attr_name, filter_value, fn).filter)


def append_str_filters(search_filter_params: ExternalItemType, target: List[ItemFilter]):
    for fn in [begins_with, contains]:
        fn_name = f'__{fn.__name__}'
        for key, filter_value in search_filter_params.items():
            if key.endswith(fn_name) and filter_value:
                attr_name = key[:-len(fn_name)]
                target.append(AttrItemFilter(attr_name, str(filter_value), fn).filter)


def append_one_of_filters(search_filter_params: ExternalItemType, target: List[ItemFilter]):
    fn_name = f'__{one_of.__name__}'
    for key, filter_value in search_filter_params.items():
        if key.endswith(fn_name) and filter_value:
            attr_name = key[:-len(fn_name)]
            target.append(AttrItemFilter(attr_name, filter_value, one_of).filter)


def composite_filter(filters: List[ItemFilter]) -> Optional[ItemFilter]:
    if not filters:
        return None
    elif len(filters) == 1:
        return filters[0]

    def filter_fn(item: ExternalItemType) -> Optional[ExternalItemType]:
        for f in filters:
            item = f(item)
            if item is None:
                return None
        return item
    return filter_fn


@dataclass
class AttrItemFilter:
    attr_name: str
    filter_value: Any
    op_fn: Callable[[Any, Any], bool]

    def filter(self, item: ExternalItemType) -> Optional[ExternalItemType]:
        attr_value = item.get(self.attr_name)
        match = self.op_fn(attr_value, self.filter_value)
        return item if match else None


def begins_with(a: Any, b: str) -> bool:
    return str(a).startswith(b)


def contains(a: Any, b: str) -> bool:
    return b in str(a)


def one_of(a: Any, b: List[Any]) -> bool:
    return a in b


def eq(a: Any, b: Any) -> bool:
    return a == b


def gt(a: Any, b: Any) -> bool:
    return a > b


def gte(a: Any, b: Any) -> bool:
    return a >= b


def lt(a: Any, b: Any) -> bool:
    return a < b


def lte(a: Any, b: Any) -> bool:
    return a <= b


def ne(a: Any, b: Any) -> bool:
    return a != b
