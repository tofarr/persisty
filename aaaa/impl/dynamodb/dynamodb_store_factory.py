from dataclasses import dataclass
from typing import Optional, Dict, Set

import boto3
from schemey import schema_from_type

from aaaa.attr.attr import Attr
from aaaa.attr.attr_filter_op import TYPE_FILTER_OPS
from aaaa.attr.attr_type import attr_type, AttrType
from aaaa.impl.dynamodb.dynamodb_index import DynamodbIndex, from_schema
from aaaa.impl.dynamodb.dynamodb_table_store import DynamodbTableStore
from aaaa.key_config.attr_key_config import AttrKeyConfig
from aaaa.store.restrict_access_store import restrict_access_store
from aaaa.store.schema_validating_store import SchemaValidatingStore
from aaaa.store.store_abc import StoreABC
from aaaa.store.store_factory_abc import StoreFactoryABC
from aaaa.store_meta import StoreMeta
from aaaa.trigger.wrapper import triggered_store
from aaaa.util import filter_none


@dataclass
class DynamodbStoreFactory(StoreFactoryABC):
    meta: Optional[StoreMeta] = None
    aws_profile_name: Optional[str] = None
    region_name: Optional[str] = None
    table_name: Optional[str] = None
    index: Optional[DynamodbIndex] = None
    global_secondary_indexes: Optional[Dict[str, DynamodbIndex]] = None
    native_triggers: bool = False

    def get_meta(self) -> StoreMeta:
        return self.meta

    def create(self) -> Optional[StoreABC]:
        store = DynamodbTableStore(
            meta=self.meta,
            table_name=self.table_name,
            index=self.index,
            global_secondary_indexes=self.global_secondary_indexes or {},
            aws_profile_name=self.aws_profile_name,
            region_name=self.region_name,
        )
        store = SchemaValidatingStore(store)
        store = restrict_access_store(store, self.meta.store_access)
        if not self.native_triggers:
            store = triggered_store(store)
        return store

    def derive_from_meta(self):
        meta = self.meta
        if self.table_name is None:
            self.table_name = meta.name
        if self.index is None:
            key_config = meta.key_config
            if isinstance(key_config, AttrKeyConfig):
                self.index = DynamodbIndex(key_config.attr_name)
        if self.global_secondary_indexes is None:
            self.global_secondary_indexes = {
                f"gix__{'__'.join(index.attr_names)}": DynamodbIndex(*index.attr_names)
                for index in meta.indexes
            }
            # Dynamodb doesn't support unique indexes on gsis, so if these are specified we'll have to check in edit

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
        attrs = tuple(
            _dynamo_attr_to_attr(a) for a in (table.get("AttributeDefinitions") or [])
        )
        key_config = self.index.key_config_from_attrs(attrs)
        self.meta = StoreMeta(
            name=self.table_name,
            attrs=attrs,
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
        attr = next(a for a in self.meta.attrs if a.name == name)
        return dict(
            AttributeName=name, AttributeType=_FIELD_TYPE_2_DYNAMODB[attr.type]
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
    AttrType.BINARY: "B",
    AttrType.STR: "S",
    AttrType.FLOAT: "N",
    AttrType.INT: "N",
    AttrType.DATETIME: "S",
    AttrType.UUID: "S",
}


def _dynamo_attr_to_attr(dynamo_attr: Dict):
    python_type = _DYNAMODB_2_PYTHON[dynamo_attr["AttributeType"]]
    db_type = attr_type(python_type)
    attr = Attr(
        name=dynamo_attr["AttributeName"],
        type=db_type,
        schema=schema_from_type(python_type),
        permitted_filter_ops=TYPE_FILTER_OPS.get(db_type)
    )
    return attr
