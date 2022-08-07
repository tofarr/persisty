from marshy.factory.impl_marshaller_factory import register_impl
from marshy.marshaller_context import MarshallerContext

from persisty.impl.sqlalchemy.search_filter.and_filter_converter import (
    AndFilterConverter,
)
from persisty.impl.sqlalchemy.search_filter.field_filter_converter import FieldFilterConverter
from persisty.impl.sqlalchemy.search_filter.not_filter_converter import NotFilterConverter
from persisty.impl.sqlalchemy.search_filter.or_filter_converter import OrFilterConverter
from persisty.impl.sqlalchemy.search_filter.query_filter_converter import QueryFilterConverter
from persisty.impl.sqlalchemy.search_filter.search_filter_converter_abc import (
    SearchFilterConverterABC,
)


def configure_converters(context: MarshallerContext):
    register_impl(SearchFilterConverterABC, AndFilterConverter)
    register_impl(SearchFilterConverterABC, FieldFilterConverter)
    register_impl(SearchFilterConverterABC, IncludeAllConverter)
    register_impl(SearchFilterConverterABC, NotFilterConverter)
    register_impl(SearchFilterConverterABC, OrFilterConverter)
    register_impl(SearchFilterConverterABC, QueryFilterConverter)
