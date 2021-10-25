from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TypeVar, Generic, Optional, Iterator, Type

from persisty.capabilities import Capabilities
from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.page import Page
from persisty.errors import PersistyError

T = TypeVar('T')
F = TypeVar('F')


@dataclass(frozen=True)
class RepoABC(ABC, Generic[T, F]):
    name: str

    @abstractmethod
    def get_item_type(self) -> Type[T]:
        """ Get the type for this Repository """

    @abstractmethod
    def get_capabilities(self) -> Capabilities:
        """ Get the current user capabilities for this repo. """

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
    def search(self, search_filter: Optional[F] = None) -> Iterator[T]:
        """ Search this repo with the filter given. """

    @abstractmethod
    def count(self, search_filter: Optional[F] = None) -> int:
        """ Get a count of the items in this repo matching filter given. """

    @abstractmethod
    def paginated_search(self,
                         search_filter: Optional[F] = None,
                         page_key: str = None,
                         limit: int = 20
                         ) -> Page[T]:
        """ Get a page of results from this repo. """

    def edit_all(self, edits: Iterator[Edit[T]]):
        """ Perform a bulk edit for items in this repo. This action is not typically atomic. """
        for edit in edits:
            if edit.edit_type == EditType.CREATE:
                self.create(edit.value)
            elif edit.edit_type == EditType.UPDATE:
                self.update(edit.value)
            else:
                self.destroy(edit.key)
