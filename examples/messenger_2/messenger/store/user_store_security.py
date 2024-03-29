import dataclasses
from uuid import UUID

from persisty.search_filter.exclude_all import EXCLUDE_ALL
from persisty.search_filter.filter_factory import ObjFilterFactory
from persisty.security.restrict_access_store import RestrictAccessStore
from persisty.security.store_access import StoreAccess
from persisty.security.store_security_abc import StoreSecurityABC, T
from persisty.store.attr_override_store import AttrOverrideStore
from persisty.store.store_abc import StoreABC
from servey.security.authorization import Authorization

from persisty.store_meta import StoreMeta


class UserStoreSecurity(StoreSecurityABC[T]):
    def get_api_meta(self, store_meta: StoreMeta) -> StoreMeta:
        # Make sure that the PasswordDigest is update only and never returned to clients.
        attrs = tuple(
            dataclasses.replace(attr, readable=False)
            if attr.name == "password_digest"
            else attr
            for attr in store_meta.attrs
        )
        store_meta = dataclasses.replace(store_meta, attrs=attrs)
        return store_meta

    def get_secured(self, store: StoreABC, authorization: Authorization) -> StoreABC:
        store = AttrOverrideStore(
            store=store, attr_name="password_digest", readable=False
        )
        filters = ObjFilterFactory(store.get_meta())
        if not authorization:
            # Public access to create new users and to read / search existing users is
            # permitted, but updates and deletes are forbidden
            return RestrictAccessStore(
                store, StoreAccess(update_filter=EXCLUDE_ALL, delete_filter=EXCLUDE_ALL)
            )
        subject_uuid = UUID(authorization.subject_id)
        if authorization.has_scope("admin"):
            return RestrictAccessStore(
                store,
                StoreAccess(
                    # Admins are not allowed to self delete
                    delete_filter=filters.id.ne(subject_uuid),
                    # Admins are not allowed to remove their own admin privileges
                    update_filter=filters.id.ne(subject_uuid) | filters.admin.eq(True),
                ),
            )

        return RestrictAccessStore(
            store,
            StoreAccess(
                delete_filter=EXCLUDE_ALL,
                # Non admins are not allowed to make themselves admins
                update_filter=filters.id.ne(subject_uuid) & filters.admin.eq(False),
            ),
        )
