import dataclasses
from dataclasses import dataclass
from typing import Optional

from servey.security.authorization import Authorization

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.attr.generator.default_value_generator import DefaultValueGenerator
from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.security.restrict_access_store import RestrictAccessStore
from persisty.security.store_access import StoreAccess
from persisty.security.store_security import UNSECURED
from persisty.security.store_security_abc import StoreSecurityABC, T
from persisty.store.attr_override_store import AttrOverrideStore
from persisty.store.store_abc import StoreABC
from persisty.store_meta import StoreMeta
from persisty.util import UNDEFINED


@dataclass
class OwnedStoreSecurity(StoreSecurityABC[T]):
    store_security: StoreSecurityABC = UNSECURED
    subject_id_attr_name: str = "subject_id"
    require_ownership_for_create: bool = True
    require_ownership_for_read: bool = False
    require_ownership_for_update: bool = True
    require_ownership_for_delete: bool = True

    def get_secured(
        self, store: StoreABC, authorization: Optional[Authorization]
    ) -> StoreABC:
        store = self.store_security.get_secured(store, authorization)
        store_access = self.get_store_access(store, authorization)
        store_access &= store.get_meta().store_access
        if not authorization:
            store_access &= StoreAccess(
                create_filter=EXCLUDE_ALL
                if self.require_ownership_for_create
                else INCLUDE_ALL,
                read_filter=EXCLUDE_ALL
                if self.require_ownership_for_read
                else INCLUDE_ALL,
                update_filter=EXCLUDE_ALL
                if self.require_ownership_for_update
                else INCLUDE_ALL,
                delete_filter=EXCLUDE_ALL
                if self.require_ownership_for_delete
                else INCLUDE_ALL,
            )
        store = RestrictAccessStore(store, store_access)

        if authorization:
            attr = next(
                attr
                for attr in store.get_meta().attrs
                if attr.name == self.subject_id_attr_name
            )
            subject_id = attr.sanitize_type(authorization.subject_id)
        else:
            subject_id = UNDEFINED

        store = AttrOverrideStore(
            store=store,
            attr_name=self.subject_id_attr_name,
            creatable=False,
            updatable=False,
            create_generator=DefaultValueGenerator(subject_id),
            update_generator=DefaultValueGenerator(subject_id),
        )
        return store

    def get_store_access(
        self, store: StoreABC, authorization: Optional[Authorization]
    ) -> StoreAccess:
        if authorization:
            search_filter = AttrFilter(
                self.subject_id_attr_name, AttrFilterOp.eq, authorization.subject_id
            )
            search_filter = search_filter.lock_attrs(store.get_meta().attrs)
        else:
            search_filter = EXCLUDE_ALL
        store_access = StoreAccess(
            read_filter=search_filter
            if self.require_ownership_for_read
            else INCLUDE_ALL,
            update_filter=search_filter
            if self.require_ownership_for_update
            else INCLUDE_ALL,
            delete_filter=search_filter
            if self.require_ownership_for_delete
            else INCLUDE_ALL,
        )
        return store_access

    def get_api_meta(self, store_meta: StoreMeta) -> StoreMeta:
        attrs = tuple(
            dataclasses.replace(
                attr,
                creatable=False,
                updatable=False,
            )
            if attr.name == self.subject_id_attr_name
            else attr
            for attr in store_meta.attrs
        )
        result = dataclasses.replace(store_meta, attrs=attrs)
        return result
