from dataclasses import dataclass
from typing import Generic, TypeVar, Type, Optional

from persisty.store_meta import StoreMeta

T = TypeVar("T")


@dataclass
class Result(Generic[T]):
    key: str
    item: T
    updatable: bool
    deletable: bool


def result_dataclass_for(type_: Type[T]) -> Type[Result[T]]:
    params = {
        "__annotations__": {
            "key": str,
            "item": type_,
            "updatable": bool,
            "deletable": bool,
        },
        "__doc__": f"Result of {type_.__name__}",
    }
    type_name = f"{type_.__name__}Result"
    # noinspection PyTypeChecker
    type_ = dataclass(type(type_name, (Result,), params))
    return type_


def to_result(item: Optional[T], store_meta: StoreMeta) -> Optional[Result[T]]:
    if not item:
        return None
    return Result(
        key=store_meta.key_config.to_key_str(item),
        item=item,
        updatable=store_meta.store_access.update_filter.match(item, store_meta.attrs),
        deletable=store_meta.store_access.delete_filter.match(item, store_meta.attrs),
    )
