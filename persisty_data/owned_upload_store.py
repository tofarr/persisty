from dataclasses import dataclass

from servey.security.authorization import Authorization

from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC
from persisty_data.upload import Upload


@dataclass
class OwnedUploadStore(WrapperStoreABC[Upload]):
    store: StoreABC[Upload]
    authorization: Authorization

    def get_store(self) -> StoreABC[Upload]:
        return self.store

    def create(self, item: Upload) -> Upload:
        prefix = self.authorization.subject_id + '/'
        if not item.content_key.startswith(prefix):
            item.content_key = prefix + item.content_key
        result = self.get_store().create(item)
        return result
