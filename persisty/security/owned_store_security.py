import dataclasses
from dataclasses import dataclass

from servey.security.authorization import Authorization

from persisty.security.owned_store import OwnedStore, meta_with_non_editable_subject_id
from persisty.security.store_access import StoreAccess
from persisty.security.store_security import UNSECURED
from persisty.security.store_security_abc import StoreSecurityABC
from persisty.store.meta_override_store import MetaOverrideStore
from persisty.store.store_abc import StoreABC


@dataclass
class OwnedStoreSecurity(StoreSecurityABC):
    store_security: StoreSecurityABC = UNSECURED
    subject_id_attr_name: str = "subject_id"
    require_ownership_for_read: bool = False
    require_ownership_for_update: bool = True
    require_ownership_for_delete: bool = True

    def get_unsecured(self, store: StoreABC) -> StoreABC:
        store = self.store_security.get_unsecured(store)
        store_meta = store.get_meta()
        attrs = []
        for attr in store_meta.attrs:
            if attr.name == self.subject_id_attr_name:
                attr = dataclasses.replace(attr, creatable=False, updatable=False)
            attrs.append(attr)
        store_meta = dataclasses.replace(
            store_meta, attrs=tuple(attrs), store_security=self
        )

        return MetaOverrideStore(store, store_meta)

    def get_secured(self, store: StoreABC, authorization: Authorization) -> StoreABC:
        store = self.store_security.get_secured(store, authorization)
        return OwnedStore(
            store=store,
            authorization=authorization,
            subject_id_attr_name=self.subject_id_attr_name,
            require_ownership_for_read=self.require_ownership_for_read,
            require_ownership_for_update=self.require_ownership_for_update,
            require_ownership_for_delete=self.require_ownership_for_delete,
        )

    def get_potential_access(self) -> StoreAccess:
        return self.store_security.get_potential_access()
