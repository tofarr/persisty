from typing import Union, Type, TypeVar, Iterator

from persisty.errors import PersistyError
from persisty.store.store_abc import StoreABC

T = TypeVar('T')


class PersistyContext:

    def __init__(self):
        self._stores_by_name = {}

    def register_store(self, store: StoreABC[T]):
        self._stores_by_name[store.name] = store

    def get_store(self, key: Union[str, Type[T]]) -> StoreABC[T]:
        if not isinstance(key, str):
            key = key.__name__
        store = self._stores_by_name.get(key)
        if store is None:
            raise PersistyError(f'missing_store:{key}')
        return store

    def get_stores(self) -> Iterator[StoreABC]:
        return iter(self._stores_by_name.values())
