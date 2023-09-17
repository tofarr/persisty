from dataclasses import dataclass
from typing import Optional, Dict, List

import marshy
from servey.security.authorization import Authorization

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.or_filter import Or
from persisty.search_filter.search_filter_factory import SearchFilterFactoryABC
from persisty.security.permission import Permission
from persisty.store.filtered_store import FilteredStore
from persisty.store.restrict_access_store import RestrictAccessStore
from persisty.store.store_abc import StoreABC
from persisty.store_access import ALL_ACCESS, NO_ACCESS
from persisty.store_meta import T, StoreMeta

I THINK WE SHOULD EXPOSE SCOPE PERMISSIONS AS PART OF THE META. THIS THEN BECOMES PART OF THE DEFAULT FACTORY
THIS ALSO GIVES SOME HOPE OF DETERMINING WHETHER AN OPERATION CAN RUN BEFORE IT IS INVOKED

WE ALSO NEED TO CHECK THE PERMISSIONS AGAINST THE BASE FOR THE STORE.

@dataclass
class PermissionStoreFactory(StoreFactoryABC):
    store_factory: StoreFactoryABC
    default_permission: Permission
    scope_permissions: Dict[str, Permission]

    def get_meta(self) -> StoreMeta:
        return self.store_factory.get_meta()

    def create(self, authorization: Optional[Authorization]) -> Optional[StoreABC[T]]:
        store = self.store_factory.create(authorization)
        permissions = self.get_permissions(authorization)
        search_filter = self.build_search_filter(permissions)
        if search_filter is not INCLUDE_ALL:
            store = FilteredStore(store, search_filter)
        store_access = self.build_store_access(permissions)
        if store_access is not ALL_ACCESS:
            store = RestrictAccessStore(store, store_access)
        return store

    def get_permissions(
        self, authorization: Optional[Authorization]
    ) -> List[Permission]:
        permissions = [self.default_permission]
        scope_permissions = (
            self.scope_permissions.get(s) for s in authorization.scopes
        )
        permissions.extend(s for s in scope_permissions if s)
        authorization_permissions = getattr(authorization, "permissions", None)
        if authorization_permissions:
            permissions.extend(authorization_permissions)
        return permissions

    def build_search_filter(self, permissions: List[Permission]):
        filters = []
        filter_cls = self.get_meta().get_search_filter_factory_dataclass()
        for permission in permissions:
            if permission.search_filter:
                search_filter_factory: SearchFilterFactoryABC = marshy.load(
                    filter_cls, permission.search_filter
                )
                search_filter = search_filter_factory.to_search_filter()
                filters.append(search_filter)
        if filters:
            return Or(tuple(filters))
        else:
            return INCLUDE_ALL

    def build_store_access(self, permissions: List[Permission]):
        result = NO_ACCESS
        for permission in permissions:
            result |= permission.store_access
        return result
