from typing import List, TypeVar, Generic, Type, Optional

from dataclasses import dataclass

T = TypeVar("T")


@dataclass
class ResultSet(Generic[T]):
    results: List[T]
    next_page_key: str = None


def result_set_dataclass_for(
    type_: Type[T], type_name: Optional[str] = None
) -> Type[ResultSet[T]]:
    params = {
        "__annotations__": {
            "results": List[type_],
            "next_page_key": Optional[str],
        },
        "__doc__": f"Result Set of {type_.__name__}",
        "next_page_key": None,
    }
    if not type_name:
        type_name = f"{type_.__name__}ResultSet"
    # noinspection PyTypeChecker
    type_ = dataclass(type(type_name, (ResultSet,), params))
    return type_
