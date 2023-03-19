import operator
from enum import Enum
from functools import partial

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller_context import MarshallerContext

from persisty.util.undefined import UNDEFINED
from persisty.attr.attr_type import AttrType


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


def _exists(a, _):
    return a not in (None, UNDEFINED)


def _not_exists(a, _):
    return a in (None, UNDEFINED)


class AttrFilterOp(Enum):
    """
    Operations which attr search_filter supports. Provides a definitive set of limited attributes that storage
    implementations may implement. (e.g.: sql, dynamodb)
    """

    contains = partial(_contains)
    endswith = partial(_endswith)
    eq = operator.eq
    exists = partial(_exists)
    gt = operator.gt
    gte = partial(_gte)
    lt = operator.lt
    lte = partial(_lte)
    ne = operator.ne
    not_exists = partial(_not_exists)
    oneof = partial(_oneof)
    startswith = partial(_startswith)

    # noinspection PyUnusedLocal
    @classmethod
    def __marshaller_factory__(cls, marshaller_context: MarshallerContext):
        return _AttrFilterOpMarshaller()


FILTER_OPS = (
    AttrFilterOp.eq,
    AttrFilterOp.exists,
    AttrFilterOp.ne,
    AttrFilterOp.not_exists,
)
SORTABLE_FILTER_OPS = FILTER_OPS + (
    AttrFilterOp.gt,
    AttrFilterOp.gte,
    AttrFilterOp.lt,
    AttrFilterOp.lte,
)
STRING_FILTER_OPS = SORTABLE_FILTER_OPS + (
    AttrFilterOp.startswith,
    AttrFilterOp.endswith,
    AttrFilterOp.contains,
)
TYPE_FILTER_OPS = {
    AttrType.BOOL: FILTER_OPS,
    AttrType.DATETIME: SORTABLE_FILTER_OPS,
    AttrType.FLOAT: SORTABLE_FILTER_OPS,
    AttrType.INT: SORTABLE_FILTER_OPS,
    AttrType.JSON: FILTER_OPS,
    AttrType.STR: STRING_FILTER_OPS,
    AttrType.UUID: FILTER_OPS,
}
NONE_TYPE = type(None)

SORTABLE_TYPES = frozenset(
    (
        AttrType.BOOL,
        AttrType.DATETIME,
        AttrType.FLOAT,
        AttrType.INT,
        AttrType.STR,
    )
)


class _AttrFilterOpMarshaller(MarshallerABC[AttrFilterOp]):
    def __init__(self):
        super().__init__(AttrFilterOp)

    def load(self, item: str) -> AttrFilterOp:
        return AttrFilterOp[item]

    def dump(self, item: AttrFilterOp) -> str:
        return item.name
