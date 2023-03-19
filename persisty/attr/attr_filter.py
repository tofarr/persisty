from dataclasses import dataclass
from typing import Tuple, Optional, Any

import marshy

from persisty.attr.attr import Attr
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.errors import PersistyError
from persisty.search_filter.search_filter_abc import SearchFilterABC, T


@dataclass(frozen=True)
class AttrFilter(SearchFilterABC[T]):
    name: str
    op: AttrFilterOp
    value: T

    def lock_attrs(self, attrs: Tuple[Attr, ...]) -> SearchFilterABC:
        try:
            attr = next(attr for attr in attrs if attr.name == self.name)
            assert attr.readable and self.op in attr.permitted_filter_ops
            value = attr.sanitize_type(self.value)
            return AttrFilter(self.name, self.op, value)
        except (StopIteration, AssertionError):
            raise PersistyError("attr_filter_invalid_for_attrs")

    def match(self, item: T, attrs: Tuple[Attr, ...] = None) -> bool:
        value = getattr(item, self.name)
        try:
            result = self.op.value(value, self.value)
            return result
        except TypeError:
            return False  # Comparison failed

    def build_filter_expression(
        self, attrs: Tuple[Attr, ...]
    ) -> Tuple[Optional[Any], bool]:
        value = self.value
        from boto3.dynamodb.conditions import Attr as DynAttr

        attr = DynAttr(self.name)
        if self.op.name in {"contains", "eq", "gt", "gte", "lt", "lte", "ne"}:
            if not isinstance(value, str):
                value = marshy.dump(value)
            condition = getattr(attr, self.op.name)(value)
            return condition, True
        elif self.op == AttrFilterOp.startswith:
            if not isinstance(value, str):
                value = marshy.dump(value)
            return attr.begins_with(value), True
        elif self.op == AttrFilterOp.exists:
            return attr.exists(), True
        elif self.op == AttrFilterOp.not_exists:
            return attr.not_exists(), True
        else:
            return None, False
