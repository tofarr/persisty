from dataclasses import dataclass
from typing import Optional, Tuple

from persisty.item.field import Field
from persisty.search_filter.no_items import NO_ITEMS
from persisty.security.authorization import Authorization
from persisty.storage.filtered_storage import FilteredStorage
from persisty.storage.storage_abc import StorageABC, T, F, S
from persisty.util.undefined import UNDEFINED


@dataclass(frozen=True)
class UserFilteredStorage(FilteredStorage[T, F, S]):
    """ Performance of this may be degraded if the filter does not have the user id field"""
    storage: StorageABC[T, F, C]
    user_id_field: Field
    authorization: Authorization

    def filter_read(self, item: T) -> Optional[T]:
        if self.user_id_field.__get__(item, item.__class__) == self.authorization.user_id:
            return item

    def filter_create(self, item: T) -> Optional[T]:
        item = self.storage_meta.item_type(**item.__dict__)
        self.user_id_field.__set__(item, self.authorization.user_id)
        return item

    def filter_filter(self, search_filter: Optional[F]) -> Tuple[Optional[F], bool]:
        filter_field_name = f"{self.user_id_field.name}__eq"
        search_filter_type = self.storage_meta.search_filter_type
        if not hasattr(search_filter_type, filter_field_name):
            return search_filter, False  # The search filter does not properly handle the constraint
        if not search_filter:
            return search_filter_type(**{filter_field_name: self.authorization.user_id}), True
        if getattr(search_filter, filter_field_name) is UNDEFINED:
            search_filter = search_filter_type(**search_filter.__dict__)
            setattr(filter_field_name, self.authorization.user_id)
            return search_filter, True
        return NO_ITEMS, True
