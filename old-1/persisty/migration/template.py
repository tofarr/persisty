from persisty.storage.storage_context import StorageContext


def migrate(storage_context: StorageContext):
    raise NotImplementedError


def rollback(storage_context: StorageContext):
    raise NotImplementedError
