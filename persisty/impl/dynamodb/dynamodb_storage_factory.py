import dataclasses
from dataclasses import dataclass, field
from typing import Optional, Dict, Set

import boto3
from marshy.types import ExternalItemType
from servey.security.authorization import Authorization
from servey.cache_control.ttl_cache_control import TtlCacheControl

from persisty.errors import PersistyError
from persisty.impl.dynamodb.dynamodb_index import DynamodbIndex, from_schema
from persisty.impl.dynamodb.dynamodb_table_storage import DynamodbTableStorage
from persisty.key_config.field_key_config import FieldKeyConfig
from persisty.obj_storage.attr import Attr
from persisty.field.field_type import FieldType
from persisty.storage.schema_validating_storage import SchemaValidatingStorage
from persisty.storage.secured_storage import secured_storage
from persisty.storage.storage_abc import StorageABC
from persisty.storage.storage_factory_abc import StorageFactoryABC
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.ttl_cache_storage import TtlCacheStorage
from persisty.trigger.wrapper import triggered_storage
from persisty.util import filter_none


@dataclass
class DynamodbStorageFactory(StorageFactoryABC):
    storage_meta: Optional[StorageMeta] = None
    aws_profile_name: Optional[str] = None
    region_name: Optional[str] = None
    table_name: Optional[str] = None
    index: Optional[DynamodbIndex] = None
    global_secondary_indexes: Optional[Dict[str, DynamodbIndex]] = None
    cache: ExternalItemType = field(default_factory=dict)
    cached_result_sets: ExternalItemType = field(default_factory=dict)
    native_triggers: bool = False

    def get_storage_meta(self) -> StorageMeta:
        return self.storage_meta

    def create(self, authorization: Optional[Authorization]) -> Optional[StorageABC]:
        storage = DynamodbTableStorage(
            storage_meta=self.storage_meta,
            table_name=self.table_name,
            index=self.index,
            global_secondary_indexes=self.global_secondary_indexes or {},
            aws_profile_name=self.aws_profile_name,
            region_name=self.region_name,
        )
        storage = SchemaValidatingStorage(storage)
        if isinstance(self.storage_meta.cache_control, TtlCacheControl):
            storage = TtlCacheStorage(
                storage,
                self.cache,
                self.cached_result_sets,
                self.storage_meta.cache_control.ttl,
            )
        storage = secured_storage(storage, authorization)
        if not self.native_triggers:
            storage = triggered_storage(storage)
        return storage

    def sanitize_storage_meta(self):
        overrides = []
        for f in self.storage_meta.fields:
            override = {}
            if f.is_sortable:
                override["is_sortable"] = False
            is_indexed = False
            if self.index and self.index.pk == f.name:
                is_indexed = True
            if self.global_secondary_indexes:
                index = next(
                    (
                        i
                        for i in self.global_secondary_indexes.values()
                        if i.pk == f.name or i.sk == f.name
                    ),
                    None,
                )
                is_indexed |= bool(index)
            if is_indexed != f.is_indexed:
                override["is_indexed"] = is_indexed
            overrides.append(override)
        key_config = self.storage_meta.key_config
        fields = tuple(
            dataclasses.replace(f, **v) if v else f
            for v, f in zip(overrides, self.storage_meta.fields)
        )
        if self.index:
            key_config = self.index.key_config_from_fields(fields)
        overridden = (
            next((True for v in overrides if v), False)
            or key_config != self.storage_meta.key_config
        )
        if overridden:
            self.storage_meta = dataclasses.replace(
                self.storage_meta, key_config=key_config, fields=fields
            )

    def derive_from_storage_meta(self):
        storage_meta = self.storage_meta
        if self.table_name is None:
            self.table_name = storage_meta.name
        if self.index is None:
            key_config = storage_meta.key_config
            if isinstance(key_config, FieldKeyConfig):
                self.index = DynamodbIndex(key_config.field_name)
        if self.global_secondary_indexes is None:
            self.global_secondary_indexes = {
                f"gix__{f.name}": DynamodbIndex(f.name)
                for f in storage_meta.fields
                if f.is_indexed and self.index.pk != f.name
            }

        # We don't really have an effective way of automatically deriving GSIs here

    def check_meta_contraints(self):
        """Check that the meta is consistent with the constraints imposed by dynamodb"""
        indexed_fields = {f.name for f in self.storage_meta.fields if f.is_indexed}
        _remove_index(indexed_fields, self.index)
        for gsi in self.global_secondary_indexes.values():
            _remove_index(indexed_fields, gsi)
        if indexed_fields:
            raise PersistyError(f"fields_should_not_be_indexed:{indexed_fields}")
        for field in self.storage_meta.fields:
            if field.is_sortable:
                raise PersistyError(f"sorting_not_supported_in_dynamodb:{field.name}")

    def get_session(self):
        kwargs = filter_none(
            dict(profile_name=self.aws_profile_name, region_name=self.region_name)
        )
        session = boto3.Session(**kwargs)
        return session

    def load_from_aws(self):
        dynamodb = self.get_session().client("dynamodb")
        table_meta = dynamodb.describe_table(TableName=self.table_name)
        table = table_meta["Table"]
        self.index = from_schema(table["KeySchema"])
        self.global_secondary_indexes = {
            i["IndexName"]: from_schema(i["KeySchema"])
            for i in (table.get("GlobalSecondaryIndexes") or [])
            if i["Projection"]["ProjectionType"] == "ALL"
            and i["IndexStatus"] == "ACTIVE"
        }
        attrs = [
            _dynamo_attr_to_attr(a) for a in (table.get("AttributeDefinitions") or [])
        ]
        key_config = self.index.key_config_from_attrs(attrs)
        self.storage_meta = StorageMeta(
            name=self.table_name,
            fields=tuple(a.to_field() for a in attrs),
            key_config=key_config,
        )

    def create_table_in_aws(self):
        dynamodb = self.get_session().client("dynamodb")

        attrs = {}
        self._attrs(self.index, attrs)
        if self.global_secondary_indexes:
            for index in self.global_secondary_indexes.values():
                self._attrs(index, attrs)

        kwargs = dict(
            AttributeDefinitions=list(attrs.values()),
            TableName=self.table_name,
            KeySchema=self.index.to_schema(),
            BillingMode="PAY_PER_REQUEST",  # Ops teams will want to look at these values
        )

        if self.global_secondary_indexes:
            kwargs["GlobalSecondaryIndexes"] = [
                {
                    "IndexName": k,
                    "KeySchema": i.to_schema(),
                    "Projection": {"ProjectionType": "ALL"},
                }
                for k, i in (self.global_secondary_indexes or {}).items()
            ]

        response = dynamodb.create_table(**kwargs)
        return response

    def _attrs(self, index: DynamodbIndex, attrs: Dict):
        attrs[index.pk] = self._attr(index.pk)
        if index.sk:
            attrs[index.sk] = self._attr(index.sk)

    def _attr(self, name: str):
        field = next(f for f in self.storage_meta.fields if f.name == name)
        return dict(
            AttributeName=name, AttributeType=_FIELD_TYPE_2_DYNAMODB[field.type]
        )


def _remove_index(indexed_fields: Set[str], index: DynamodbIndex):
    indexed_fields.remove(index.pk)
    if index.sk:
        indexed_fields.remove(index.sk)


_DYNAMODB_2_PYTHON = {
    "B": bytearray,
    "N": float,
    "S": str,
}

_FIELD_TYPE_2_DYNAMODB = {
    FieldType.BINARY: "B",
    FieldType.STR: "S",
    FieldType.FLOAT: "N",
    FieldType.INT: "N",
    FieldType.DATETIME: "S",
    FieldType.UUID: "S",
}


def _dynamo_attr_to_attr(dynamo_attr: Dict):
    attr = Attr(
        name=dynamo_attr["AttributeName"],
        type=_DYNAMODB_2_PYTHON[dynamo_attr["AttributeType"]],
    )
    return attr
