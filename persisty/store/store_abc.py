from abc import ABC, abstractmethod
from typing import TypeVar, Generic, Optional, Iterator, Type

from persisty.capabilities import Capabilities
from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.page import Page
from persisty.errors import PersistyError
from persisty.search_filter import SearchFilter

T = TypeVar('T')


class StoreABC(ABC, Generic[T]):

    @property
    @abstractmethod
    def name(self) -> str:
        """ Get the name for this store """

    @property
    @abstractmethod
    def item_type(self) -> Type[T]:
        """ Get the type for this Store """

    @property
    @abstractmethod
    def capabilities(self) -> Capabilities:
        """ Get the current user capabilities for this store. """

    @abstractmethod
    def get_key(self, item: T) -> str:
        """ Generate a string which may be used to retrieve an item. (Typically fast local operation) """

    @abstractmethod
    def create(self, item: T) -> str:
        """ Create the item given and return a key for it. Raise a persistence error if anything went wrong. """

    @abstractmethod
    def read(self, key: str) -> Optional[T]:
        """ Read and return the item for the key given. If there was no such mapping return None. """

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        """ Read all the keys given, preserving order. If error_on_missing is Falsy, emit None for missing items """
        for key in keys:
            item = self.read(key)
            if item is None and error_on_missing:
                raise PersistyError(f'missing_item:{key}')
            yield item

    @abstractmethod
    def update(self, item: T) -> T:
        """
        Update the content at the key given with the item given. This is a full replace of the item.
        Return the newest version of the item. Raise a persistence error if the item did not previously
        exist
        """

    @abstractmethod
    def destroy(self, key: str) -> bool:
        """
        Destroy the item with the key given. Return True if the item existed and was destroyed, False otherwise
        """

    @abstractmethod
    def search(self, search_filter: Optional[SearchFilter[T]] = None) -> Iterator[T]:
        """ Search this store with the filter given. """

    @abstractmethod
    def count(self, search_filter: Optional[SearchFilter[T]] = None) -> int:
        """ Get a count of the items in this store matching filter given. """

    @abstractmethod
    def paged_search(self,
                     search_filter: Optional[SearchFilter[T]] = None,
                     page_key: Optional[str] = None,
                     limit: int = 20
                     ) -> Page[T]:
        """ Get a page of results from this store. """

    def edit_all(self, edits: Iterator[Edit[T]]):
        """ Perform a bulk edit for items in this store. This action is not typically atomic. """
        edits = iter(edits)
        for edit in edits:
            if edit.edit_type == EditType.CREATE:
                self.create(edit.item)
            elif edit.edit_type == EditType.UPDATE:
                self.update(edit.item)
            else:
                self.destroy(edit.key)
