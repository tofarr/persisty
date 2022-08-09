from marshy.factory.impl_marshaller_factory import register_impl
from marshy.marshaller_context import MarshallerContext

from persisty.impl.sqlalchemy.search_filter.and_filter_converter import (
    AndFilterConverter,
)
from persisty.impl.sqlalchemy.search_filter.field_filter_converter import (
    FieldFilterConverter,
)
from persisty.impl.sqlalchemy.search_filter.include_all_converter import (
    IncludeAllConverter,
)
from persisty.impl.sqlalchemy.search_filter.not_filter_converter import (
    NotFilterConverter,
)
from persisty.impl.sqlalchemy.search_filter.or_filter_converter import OrFilterConverter
from persisty.impl.sqlalchemy.search_filter.query_filter_converter import (
    QueryFilterConverter,
)
from persisty.impl.sqlalchemy.search_filter.search_filter_converter_abc import (
    SearchFilterConverterABC,
)


def configure_converters(context: MarshallerContext):
    register_impl(SearchFilterConverterABC, AndFilterConverter, context)
    register_impl(SearchFilterConverterABC, FieldFilterConverter, context)
    register_impl(SearchFilterConverterABC, IncludeAllConverter, context)
    register_impl(SearchFilterConverterABC, NotFilterConverter, context)
    register_impl(SearchFilterConverterABC, OrFilterConverter, context)
    register_impl(SearchFilterConverterABC, QueryFilterConverter, context)