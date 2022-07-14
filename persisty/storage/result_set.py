from typing import List, TypeVar, Generic

T = TypeVar('T')


class ResultSet(Generic[T]):
    results: List[T]
    next_page_key: str = None
