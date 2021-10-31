from dataclasses import dataclass
from enum import Enum
from functools import partial
import operator
from typing import Generic, TypeVar

from persisty.item_filter.item_filter_abc import ItemFilterABC

A = TypeVar('A')
B = TypeVar('B')


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


class AttrFilterOp(Enum):
    """
    Operations which attr filter supports. Provides a definitive set of limited attributes that store implementations
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
class AttrFilter(ItemFilterABC[A], Generic[A, B]):
    attr: str
    op: AttrFilterOp
    value: B

    def match(self, item: A) -> bool:
        value = getattr(item, self.attr)
        try:
            result = self.op.value(value, self.value)
            return result
        except TypeError:
            return False  # Comparison failed
