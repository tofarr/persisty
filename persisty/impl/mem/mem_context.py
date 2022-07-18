import dataclasses
from typing import Optional, List, Dict

from dataclasses import field, dataclass

from marshy import get_default_context
from marshy.marshaller_context import MarshallerContext

from persisty.access_control.factory.access_control_factory import READ_ONLY_ACCESS_FACTORY
from persisty.access_control.factory.access_control_factory_abc import AccessControlFactoryABC
from persisty.access_control.obj_access_control_abc import ObjAccessControl
from persisty.context.persisty_context_abc import PersistyContextABC
from persisty.context.obj_storage_meta import MetaStorageABC
from persisty.context.obj_timestamp import TimestampStorageABC, TimestampUpdateStorage, timestamp_storage
from persisty.access_control.authorization import Authorization
from persisty.impl.mem.mem_meta_storage import MemMetaStorage

from persisty.storage.logging_storage import logging_storage
from persisty.storage.schema_validating_storage import SchemaValidatingStorage
from persisty.storage.secured_storage import SecuredStorage
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta
from persisty.util import get_logger

LOGGER = get_logger(__name__)


@dataclass
class MemContext(PersistyContextABC):
    """ PersistyContext backed by local memory. Mostly used for mocking / testing. """
    access_control_factories: List[AccessControlFactoryABC] = field(default_factory=list)
    storage: MemMetaStorage = field(default_factory=MemMetaStorage)
    marshaller_context: MarshallerContext = field(default_factory=get_default_context)
    logging: bool = False
    timestamp_storage: Optional[StorageABC] = None
    meta_access_control_factory: AccessControlFactoryABC = READ_ONLY_ACCESS_FACTORY

    def get_storage(self, storage_name: str, authorization: Authorization) -> Optional[StorageABC]:
        storage = self.storage.storage.get(storage_name)
        if not storage:
            return None

        access_control = self.create_access_control(storage_name, authorization)
        storage_meta = dataclasses.replace(storage.storage_meta, access_control=access_control)
        storage = SecuredStorage(storage, storage_meta)
        storage = SchemaValidatingStorage(storage)

        if self.logging:
            storage = logging_storage(storage, LOGGER)

        if self.timestamp_storage:
            storage = TimestampUpdateStorage(storage, self.timestamp_storage)

        return storage

    def get_meta_storage(self, authorization: Authorization) -> MetaStorageABC:
        access_control = self.meta_access_control_factory.create_access_control('meta', authorization)
        access_control = ObjAccessControl(access_control, self.marshaller_context.get_marshaller(StorageMeta))
        storage = MemMetaStorage(access_control, self.storage.storage)
        return storage

    # noinspection PyMethodMayBeStatic, PyUnusedLocal
    def get_timestamp_storage(self, authorization: Authorization) -> Optional[TimestampStorageABC]:
        if not self.timestamp_storage:
            return None
        access_control = self.meta_access_control_factory.create_access_control('timestamp', authorization)
        storage_meta = dataclasses.replace(self.timestamp_storage.storage_meta, access_control=access_control)
        storage = SecuredStorage(self.timestamp_storage, storage_meta)
        storage = timestamp_storage(storage, self.marshaller_context)
        return storage

    def create_access_control(self, storage_name: str, authorization: Authorization) -> AccessControlABC:
        for factory in self.access_control_factories:
            access_control = factory.create_access_control(storage_name, authorization)
            if access_control:
                return access_control
