import dataclasses
from typing import Type

from persisty.item_filter.attr_filter import AttrFilter, AttrFilterOp, A, B


def attr_filter_from(attr_name: str, value: B, filter_type: Type[A]) -> AttrFilter[A, B]:
    parts = attr_name.split('__')
    if len(parts) == 2:
        attr = parts[0]
        _check_attr_exists(attr, filter_type)
        op = parts[1]
        op = AttrFilterOp[op]
        return AttrFilter(attr, op, value)


def _check_attr_exists(attr_name: str, filtered_type: Type[A]):
    next(f for f in dataclasses.fields(filtered_type) if f.name == attr_name)
