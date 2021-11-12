import dataclasses
from typing import Union, List, Type

from persisty2.item_comparator.attr_comparator import AttrComparator
from persisty2.item_comparator.item_comparator_abc import ItemComparatorABC, T
from persisty2.item_comparator.multi_comparator import MultiComparator


def item_comparator_from_value(value: Union[str, List[str]], filtered_type: Type[T]) -> ItemComparatorABC[T]:
    if isinstance(value, str):
        _check_attr_exists(value, filtered_type)
        return AttrComparator[T](value)
    elif isinstance(value, list):
        return MultiComparator(tuple(item_comparator_from_value(v, filtered_type) for v in value))
    else:
        raise ValueError(f'invalid_type:{value}')


def _check_attr_exists(attr_name: str, filtered_type: Type[T]):
    next(f for f in dataclasses.fields(filtered_type) if f.name == attr_name)
