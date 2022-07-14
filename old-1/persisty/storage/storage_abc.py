from abc import ABC, abstractmethod
from typing import Optional, Iterator, Generic, TypeVar, Type

from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.errors import PersistyError
from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.page import Page
from persisty.storage.storage_filter import StorageFilter
from persisty.storage.storage_meta import StorageMeta

T = TypeVar('T')


class StorageABC(ABC, Generic[T]):

    @property
    @abstractmethod
    def item_type(self) -> Type[T]:
        """ Get the type of item for this storage """

    @property
    @abstractmethod
    def meta(self) -> StorageMeta:
        """ Get the metadata for this storage """

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
    def search(self, storage_filter: Optional[StorageFilter[T]] = None) -> Iterator[T]:
        """ Search this storage with the filter given. """

    @abstractmethod
    def count(self, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        """ Get a count of the items in this storage matching filter given. """

    @abstractmethod
    def paged_search(self,
                     storage_filter: Optional[StorageFilter[T]] = None,
                     page_key: Optional[str] = None,
                     limit: int = 20
                     ) -> Page[T]:
        """ Get a page of results from this storage. """

    def edit_all(self, edits: Iterator[Edit[T]]):
        """ Perform a bulk edit for items in this storage. This action is not typically atomic. """
        edits = iter(edits)
        for edit in edits:
            if edit.edit_type == EditType.CREATE:
                self.create(edit.item)
            elif edit.edit_type == EditType.UPDATE:
                self.update(edit.item)
            else:
                self.destroy(edit.key)
