from dataclasses import dataclass
from enum import Enum
from functools import partial
import operator
from typing import Tuple, Optional, Any

from marshy import ExternalType
from marshy.types import ExternalItemType

from persisty.storage.field.field import Field
from persisty.storage.search_filter.search_filter_abc import SearchFilterABC


def _gte(a, b):
    return a >= b


def _lte(a, b):
    return a <= b


def _contains(a, b):
    return str(b) in str(a)


def _startswith(a, b):
    return str(a).startswith(str(b))


def _endswith(a, b):
    return str(a).endswith(str(b))


def _oneof(a, b):
    return a in b


class FieldFilterOp(Enum):
    """
    Operations which attr search_filter supports. Provides a definitive set of limited attributes that storage implementations
    may implement. (e.g.: sql, dynamodb)
    """
    contains = partial(_contains)
    endswith = partial(_endswith)
    eq = operator.eq
    gt = operator.gt
    gte = partial(_gte)
    lt = operator.lt
    lte = partial(_lte)
    ne = operator.ne
    oneof = partial(_oneof)
    startswith = partial(_startswith)


@dataclass(frozen=True)
class FieldFilter(SearchFilterABC):
    name: str
    op: FieldFilterOp
    value: ExternalType

    def validate_for_fields(self, fields: Tuple[Field, ...]):
        try:
            field = next(field for field in fields if field.name == self.name)
            assert field.is_readable and self.op in field.permitted_filter_ops
        except (StopIteration, AssertionError):
            raise ValueError('field_filter_invalid_for_fields')

    def match(self, item: ExternalItemType, fields: Tuple[Field, ...] = None) -> bool:
        value = item.get(self.name)
        try:
            result = self.op.value(value, self.value)
            return result
        except TypeError:
            return False  # Comparison failed

    def build_filter_expression(self, fields: Tuple[Field, ...]) -> Tuple[Optional[Any], bool]:
        from boto3.dynamodb.conditions import Attr
        attr = Attr(self.name)
        if self.op in {'contains', 'eq', 'gt', 'gte', 'lt', 'lte', 'ne'}:
            condition = getattr(attr, self.op.name)(self.value)
            return condition, True
        elif self.op == FieldFilterOp.startswith:
            return attr.begins_with(self.value), True
        else:
            return None, False
