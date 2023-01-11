from dataclasses import dataclass
from typing import Tuple, Optional, Any

from aaaa.attr.attr import Attr
from aaaa.attr.attr_filter_op import AttrFilterOp
from aaaa.errors import PersistyError
from aaaa.search_filter.search_filter_abc import SearchFilterABC, T


@dataclass(frozen=True)
class AttrFilter(SearchFilterABC, T):
    name: str
    op: AttrFilterOp
    value: T

    def lock_attrs(self, attrs: Tuple[Attr, ...]) -> SearchFilterABC:
        try:
            attr = next(attr for attr in attrs if attr.name == self.name)
            assert attr.readable and self.op in attr.permitted_filter_ops
            return self
        except (StopIteration, AssertionError):
            raise PersistyError("attr_filter_invalid_for_attrs")

    def match(self, item: T, attrs: Tuple[Attr, ...] = None) -> bool:
        value = item.get(self.name)
        try:
            result = self.op.value(value, self.value)
            return result
        except TypeError:
            return False  # Comparison failed

    def build_filter_expression(
        self, attrs: Tuple[Attr, ...]
    ) -> Tuple[Optional[Any], bool]:
        from boto3.dynamodb.conditions import Attr

        attr = Attr(self.name)
        if self.op.name in {"contains", "eq", "gt", "gte", "lt", "lte", "ne"}:
            condition = getattr(attr, self.op.name)(self.value)
            return condition, True
        elif self.op == AttrFilterOp.startswith:
            return attr.begins_with(self.value), True
        elif self.op == AttrFilterOp.exists:
            return attr.exists()
        elif self.op == AttrFilterOp.not_exists:
            return attr.not_exists()
        else:
            return None, False
