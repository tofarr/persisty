import os
from typing import Tuple, Optional

from servey.cache_control.cache_control_abc import CacheControlABC
from servey.cache_control.secure_hash_cache_control import SecureHashCacheControl
from servey.security.authorization import Authorization
from servey.servey_aws import is_lambda_env

from persisty.access_control.factory.access_control_factory_abc import AccessControlFactoryABC
from persisty.data_storage.data_storage_abc import DataStorageABC
from persisty.data_storage.data_storage_factory_abc import DataStorageFactoryABC
from persisty.data_storage.directory_data_storage import DirectoryDataStorage
from persisty.errors import PersistyError
from persisty.storage.storage_meta import DEFAULT_ACCESS_CONTROL_FACTORIES
from persisty.util import to_snake_case


class DefaultDataStorageFactory(DataStorageFactoryABC):
    """
    When you need a place to store things, and you are not super picky about where! We'll use S3 if available,
    but otherwise a local directory will do.
    """
    name: str
    access_control_factories: Tuple[AccessControlFactoryABC, ...] = DEFAULT_ACCESS_CONTROL_FACTORIES
    cache_control: CacheControlABC = SecureHashCacheControl()

    def create(self, authorization: Authorization) -> DataStorageABC:
        access_control = next(
            a for a in
            (f.create_access_control(authorization) for f in self.access_control_factories)
            if a
        )
        storage_bucket = os.environ.get(f'PERSISTY_S3_BUCKET_{to_snake_case(self.name).upper()}')
        if not storage_bucket:
            storage_bucket = os.environ.get(f'PERSISTY_S3_GLOBAL_BUCKET')
        if storage_bucket:
            storage = S3DataStorage(storage_bucket)
            return storage
        if is_lambda_env():
            raise PersistyError('directory_storage_in_lambda')
        storage = DirectoryDataStorage(
            directory_path=os.environ.get("PERSISTY_DATA_STORAGE_PATH") or 'data_storage',
            access_control=access_control,
            cache_control=self.cache_control
        )
        return storage
