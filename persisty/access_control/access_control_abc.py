from abc import ABC, abstractmethod
from typing import Tuple

from marshy.types import ExternalItemType

from persisty.search_filter.search_filter_abc import SearchFilterABC


class AccessControlABC(ABC):

    @abstractmethod
    def is_creatable(self, item: ExternalItemType) -> bool:
        """ Determine if the stored given may be created """

    @abstractmethod
    def is_readable(self, item: ExternalItemType) -> bool:
        """ Determine if the stored given should be returned for read """

    @abstractmethod
    def is_updatable(self, old_item: ExternalItemType, updates: ExternalItemType) -> bool:
        """ Determine if the stored given may be updated """

    @abstractmethod
    def is_deletable(self, item: ExternalItemType) -> bool:
        """ Determine if the stored given may be updated """

    @abstractmethod
    def is_searchable(self) -> bool:
        """ Determine if the store is searchable """

    @abstractmethod
    def transform_search_filter(self, search_filter: SearchFilterABC) -> Tuple[SearchFilterABC, bool]:
        """
        Transform the search filter given so that it conforms to the access control constraints.
        (For example, if access control specifies that only items with an attribute owner_id matching a preset value,
        this will do it)

        The flag passed back indicates whether or not this filter now fully encapsulates this access control constraint
        (Or if further post processing will be required)
        """
