import dataclasses
from typing import Optional, List, Dict

from dataclasses import dataclass, field

from schemey import get_default_schema_context, SchemaContext

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.access_control.factory.access_control_factory_abc import (
    AccessControlFactoryABC,
)
from persisty.context.meta_storage_abc import MetaStorageABC
from persisty.access_control.authorization import Authorization
from persisty.obj_storage.obj_meta_storage import obj_meta_storage, ObjMetaStorage
from persisty.obj_storage.obj_storage import ObjStorage
from persisty.obj_storage.obj_storage_meta import ObjStorageMeta, build_obj_storage_meta
from persisty.storage.secured_storage import SecuredStorage
from persisty.storage.storage_abc import StorageABC


@dataclass(frozen=True)
class PersistyContext:
    """Links dynamic access control with meta storage"""

    meta_storage: MetaStorageABC
    access_control_factories: List[AccessControlFactoryABC] = field(
        default_factory=list
    )
    schema_context: SchemaContext = field(default_factory=get_default_schema_context)
    obj_storage_meta_cache: Dict[str, ObjStorageMeta] = field(default_factory=dict)

    def register_access_control_factory(
        self, access_control_factory: AccessControlFactoryABC
    ):
        self.access_control_factories.append(access_control_factory)
        self.access_control_factories.sort(key=lambda f: f.key())

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

    def get_obj_storage(self, storage_name: str, authorization: Authorization):
        storage = self.get_storage(storage_name, authorization)
        if not storage:
            return
        storage_meta = storage.get_storage_meta()
        obj_storage_meta = self.obj_storage_meta_cache.get(storage_meta.name)
        if not obj_storage_meta:
            obj_storage_meta = build_obj_storage_meta(storage_meta, self.schema_context)
            self.obj_storage_meta_cache[storage_meta.name] = obj_storage_meta
        return ObjStorage(storage, obj_storage_meta)

    def get_meta_storage(self, authorization: Authorization) -> StorageABC:
        access_control = self.create_access_control("meta", authorization)
        storage = self.meta_storage
        storage_meta = dataclasses.replace(
            storage.get_storage_meta(), access_control=access_control
        )
        return SecuredStorage(storage, storage_meta)

    def get_obj_meta_storage(self, authorization: Authorization) -> ObjMetaStorage:
        return obj_meta_storage(self.get_meta_storage(authorization))

    def create_access_control(
        self, storage_name: str, authorization: Authorization
    ) -> AccessControlABC:
        for factory in self.access_control_factories:
            access_control = factory.create_access_control(storage_name, authorization)
            if access_control:
                return access_control
