from typing import Optional

from persisty.storage.storage_factory_abc import StorageFactoryABC


def export_all(directory: str, include_content: bool = True):
    pass


def export_storage(directory: str, storage_factory: StorageFactoryABC, include_content: bool = True):
    pass


def import_all(directory: str, include_content: Optional[bool] = None):
    pass


def import_storage(directory: str, storage_factory: StorageFactoryABC, include_content: Optional[bool] = None):
    pass
