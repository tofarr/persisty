from typing import List, TypeVar, Generic

from dataclasses import dataclass

T = TypeVar('T')


@dataclass
class ResultSet(Generic[T]):
    results: List[T]
    next_page_key: str = None
