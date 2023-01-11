from dataclasses import dataclass, attr
from typing import List, Tuple, Any

from marshy import get_default_context
from marshy.factory.impl_marshaller_factory import ImplMarshallerFactory
from sqlalchemy import Table

from aaaa.impl.sqlalchemy.search_filter.search_filter_converter_abc import (
    SearchFilterConverterABC,
)
from aaaa.search_filter.search_filter_abc import SearchFilterABC
from aaaa.store_meta import StoreMeta


def get_converters() -> List[SearchFilterConverterABC]:
    marshaller_context = get_default_context()
    factories = marshaller_context.get_factories()
    for factory in factories:
        if isinstance(factory, ImplMarshallerFactory):
            if factory.base == SearchFilterConverterABC:
                converters = [impl() for impl in factory.impls]
                converters.sort(key=lambda i: i.priority)
                return converters


@dataclass
class SearchFilterConverterContext:
    converters: List[SearchFilterConverterABC] = attr(default_factory=get_converters)

    def convert(
        self, search_filter: SearchFilterABC, table: Table, store_meta: StoreMeta
    ) -> Tuple[Any, bool]:
        for converter in self.converters:
            result = converter.convert(search_filter, table, store_meta, self)
            if result:
                return result
        return None, False
