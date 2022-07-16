from abc import abstractmethod
from dataclasses import dataclass
from itertools import islice
from typing import Optional, List, Iterator, TypeVar, Generic, Type

from marshy.types import ExternalItemType

from persisty.storage.batch_edit import BatchEditABC
from persisty.storage.batch_edit_result import BatchEditResult
from persisty.storage.result_set import ResultSet

T = TypeVar('T')
F = TypeVar('F')
S = TypeVar('S')
C = TypeVar('C')
U = TypeVar('U')


@dataclass(frozen=True)
class ObjStorageABC(Generic[T, F, S, C, U]):

    @abstractmethod
    @property
    def item_type(self) -> Type[T]:
        """ Get the type for items returned """

    @abstractmethod
    @property
    def search_filter_type(self) -> Type[F]:
        """ Get the type for items returned """

    @abstractmethod
    @property
    def search_order_type(self) -> Type[S]:
        """ Get the type for items returned """

    @abstractmethod
    @property
    def create_input_type(self) -> Type[C]:
        """ Get the type for items returned """

    @abstractmethod
    @property
    def update_input_type(self) -> Type[U]:
        """ Get the type for items returned """

    @abstractmethod
    @property
    def batch_size(self) -> int:
        """ Get the batch size for reads / edits """

    @abstractmethod
    def create(self, item: C) -> T:
        """ Create an item """

    @abstractmethod
    def read(self, key: str) -> Optional[ExternalItemType]:
        """ Read an item from the data store """

    async def read_batch(self, keys: List[str]) -> List[Optional[T]]:
        items = [self.read(key) for key in keys]
        return items

    def read_all(self, keys: Iterator[str]) -> Iterator[Optional[T]]:
        keys = iter(keys)
        while True:
            batch_keys = list(islice(keys, self.batch_size))
            if not batch_keys:
                return
            items = self.read_batch(batch_keys)
            yield from items

    @abstractmethod
    def update(self, updates: U) -> Optional[T]:
        """ Create an item in the data store """

    @abstractmethod
    def delete(self, key: str) -> bool:
        """ Delete an item from the data store. """

    @abstractmethod
    def search(self,
               search_filter: Optional[F] = None,
               search_order: Optional[S] = None,
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[T]:
        """ Search this storage """

    @abstractmethod
    def search_all(self,
                   search_filter: Optional[F] = None,
                   search_order: Optional[S] = None
                   ) -> Iterator[ExternalItemType]:
        """ Stream all matching items """

    @abstractmethod
    def count(self, search_filter: Optional[S] = None) -> int:
        """ Get a count of all matching items """

    @abstractmethod
    async def edit_batch(self, edits: List[BatchEditABC]) -> List[BatchEditResult]:
        """
        Do a batch edit and return a list of results. The results should contain all the same edits in the same
        order
        """

    @abstractmethod
    def edit_all(self, edits: Iterator[BatchEditABC]):
        edits = iter(edits)
        while True:
            page = list(islice(edits, self.batch_size))
            if not page:
                break
            results = self.edit_batch(page)
            yield from results
