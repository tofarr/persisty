import dataclasses
from typing import Optional, List, Iterator

from dataclasses import dataclass, field

from schemey import get_default_schema_context, SchemaContext

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.access_control.factory.access_control_factory_abc import (
    AccessControlFactoryABC,
)
from persisty.context.meta_storage_abc import MetaStorageABC
from persisty.access_control.authorization import Authorization
from persisty.errors import PersistyError
from persisty.storage.secured_storage import SecuredStorage
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_meta import StorageMeta


@dataclass(frozen=True)
class PersistyContext:
    """Links dynamic access control with meta storage"""

    meta_storage: MetaStorageABC
    access_control_factories: List[AccessControlFactoryABC] = field(
        default_factory=list
    )
    schema_context: SchemaContext = field(default_factory=get_default_schema_context)

    def register_access_control_factory(
        self, access_control_factory: AccessControlFactoryABC
    ):
        self.access_control_factories.append(access_control_factory)
        self.access_control_factories.sort(key=lambda f: f.priority, reverse=True)

    def get_storage(
        self, storage_name: str, authorization: Authorization
    ) -> Optional[StorageABC]:
        storage = self.meta_storage.get_item_storage(storage_name)
        if not storage:
            return None
        access_control = self.create_access_control(storage_name, authorization)
        storage_meta = dataclasses.replace(
            storage.get_storage_meta(), access_control=access_control
        )
        return SecuredStorage(storage, storage_meta)

    def create_storage(self, storage_meta: StorageMeta, authorization: Authorization):
        dumped = self.schema_context.marshaller_context.dump(storage_meta, StorageMeta)
        self.get_meta_storage(authorization).create(dumped)
        storage = self.get_storage(storage_meta.name, authorization)
        return storage

    def get_meta_storage(self, authorization: Authorization) -> StorageABC:
        access_control = self.create_access_control("meta", authorization)
        storage = self.meta_storage
        storage_meta = dataclasses.replace(
            storage.get_storage_meta(), access_control=access_control
        )
        return SecuredStorage(storage, storage_meta)

    def admin_get_storage_meta(self, storage_name: str) -> StorageMeta:
        storage_meta = self.meta_storage.read(storage_name)
        if storage_meta:
            storage_meta = self.schema_context.marshaller_context.load(
                StorageMeta, storage_meta
            )
            return storage_meta

    def admin_get_all_storage_meta(self) -> Iterator[StorageMeta]:
        marshaller = self.schema_context.marshaller_context.get_marshaller(StorageMeta)
        for storage_meta in self.meta_storage.search_all():
            storage_meta = marshaller.load(storage_meta)
            yield storage_meta

    def create_access_control(
        self, storage_name: str, authorization: Authorization
    ) -> AccessControlABC:
        for factory in self.access_control_factories:
            access_control = factory.create_access_control(storage_name, authorization)
            if access_control:
                return access_control
        raise PersistyError(
            f"create_access_control_failed:{storage_name}:{authorization}"
        )
