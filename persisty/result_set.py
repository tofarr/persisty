from typing import List, TypeVar, Generic, Type, Optional

from dataclasses import dataclass

T = TypeVar("T")


@dataclass
class ResultSet(Generic[T]):
    results: List[T]
    next_page_key: str = None


def result_set_dataclass_for(type_: Type[T]) -> Type[ResultSet[T]]:
    params = {
        "__annotations__": {
            "results": List[type_],
            "next_page_key": Optional[str],
        },
        "next_page_key": None,
    }
    type_name = f"{type_.__name__}ResultSet"
    type_ = dataclass(type(type_name, (ResultSet,), params))
    return type_
