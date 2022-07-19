from persisty.context.persisty_context_abc import PersistyContextABC
from persisty.context.obj_storage_meta import MetaStorageABC
from persisty.access_control.authorization import Authorization
from persisty.storage.storage_abc import StorageABC


class DynamodbContext(PersistyContextABC):
    """
    Persisty context based on dynamodb.
    """

    def get_storage(self, name: str, authorization: Authorization) -> StorageABC:
        pass

    def get_meta_storage(self, authorization: Authorization) -> MetaStorageABC:
        pass
