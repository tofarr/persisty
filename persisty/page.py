from dataclasses import dataclass
from typing import TypeVar, Generic, Union, Iterable, Sized, Optional

T = TypeVar('T')


@dataclass(frozen=True)
class Page(Generic[T]):
    items: Union[Iterable[T], Sized]
    next_page_key: Optional[str] = None
