from dataclasses import dataclass
from typing import Optional

from servey.security.authorization import Authorization

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.attr.generator.fixed_value_generator import FixedValueGenerator
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.security.restrict_access_store import RestrictAccessStore
from persisty.security.store_access import StoreAccess
from persisty.security.store_security import UNSECURED
from persisty.security.store_security_abc import StoreSecurityABC, T
from persisty.store.attr_override_store import AttrOverrideStore
from persisty.store.store_abc import StoreABC


@dataclass
class OwnedStoreSecurity(StoreSecurityABC[T]):
    store_security: StoreSecurityABC = UNSECURED
    subject_id_attr_name: str = "subject_id"
    required_ownership_for_create: bool = True
    require_ownership_for_read: bool = False
    require_ownership_for_update: bool = True
    require_ownership_for_delete: bool = True

    def get_secured(self, store: StoreABC, authorization: Optional[Authorization]) -> StoreABC:
        store = self.store_security.get_secured(store, authorization)
        store_access = self.get_store_access(authorization)
        store_access &= store.get_meta().store_access
        store = RestrictAccessStore(store, store_access)
        store = AttrOverrideStore(
            store=store,
            attr_name=self.subject_id_attr_name,
            create_generator=FixedValueGenerator(authorization.subject_id),
            creatable=False,
            updatable=False
        )
        return store

    def get_store_access(self, authorization: Optional[Authorization]) -> StoreAccess:
        if authorization:
            search_filter = AttrFilter(self.subject_id_attr_name, AttrFilterOp.eq, authorization.subject_id)
        else:
            search_filter = EXCLUDE_ALL
        store_access = StoreAccess(
            create_filter=search_filter if self.required_ownership_for_create else INCLUDE_ALL,
            read_filter=search_filter if self.require_ownership_for_read else INCLUDE_ALL,
            update_filter = search_filter if self.require_ownership_for_update else INCLUDE_ALL,
            delete_filter = search_filter if self.require_ownership_for_delete else INCLUDE_ALL,
        )
        return store_access

    def get_api_access(self) -> StoreAccess:
        return self.store_security.get_api_access()
