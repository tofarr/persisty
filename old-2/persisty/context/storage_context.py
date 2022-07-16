from typing import Optional, Iterator

from persisty.context.context_search_filter import ContextSearchFilter
from persisty.context.context_search_order import ContextSearchOrder
from persisty.context.storage_factory_abc import StorageFactoryABC
from persisty.security.authorization import Authorization
from persisty.storage.result_set import ResultSet
from persisty.storage.storage_abc import StorageABC


class StorageContext:
    logging: bool = False

    def register_factory(self, storage_factory: StorageFactoryABC):
        pass

    def get_storage(self, name: str, authorization: Authorization) -> Optional[StorageABC]:
        pass

    HOW DO WE SEARCH GIVEN THE CURRENT PATTERN? LIKE, IT ONLY REALLY DEFINES ONE WAY TO SEARCH.
    HOW DOES THE FACTORY WORK? SHOULD IT GET MORE META

    MAYBE THIS IS OKAY? WE DONT GET FULLY DESCRIPTIVE AND FLEXIBLE.

    I AM STILL NOT COMFORTABLE WITH ACCESS_CONTROL. IT FEELS LIKE IT IS IN THE WRONG PLACE, AND DOESNT GIVE ENOUGH
    FLEXIBILITY

    SUPPOSE WE SIMPLY SAY FORGET ALL TYPE SAFETY AND DEFINE A SIMPLE STRUCTURE WHAT WOULD THAT LOOK LIKE?

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
