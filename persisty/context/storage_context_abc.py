from abc import abstractmethod, ABC
from typing import Optional, Iterator

from persisty.registry.external_storage_meta import StorageDescriptor

from persisty.context.context_search_filter import ContextSearchFilter
from persisty.context.context_search_order import ContextSearchOrder
from persisty.security.authorization import Authorization
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_abc import StorageABC


class StorageContextABC(ABC):

    @abstractmethod
    def read(self, name: str, authorization: Authorization) -> Optional[StorageABC]:
        """ Get storage """

    @abstractmethod
    def search(self,
               search_filter: Optional[ContextSearchFilter] = None,  # Query on name?
               search_order: Optional[ContextSearchOrder] = None,  # Sort on name or not at all?
               page_key: Optional[str] = None,
               limit: Optional[int] = None
               ) -> ResultSet[StorageABC]:
        """ Get a page of storage contexts """

    @abstractmethod
    def search_all(self,
               search_filter: Optional[ContextSearchFilter] = None,  # Query on name?
               search_order: Optional[ContextSearchOrder] = None,  # Sort on name or not at all?
               ) -> Iterator[StorageABC]:
        """ Get an iterator over storage contexts """
