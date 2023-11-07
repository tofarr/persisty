import os
from dataclasses import dataclass, field
from typing import Optional, Dict, Set, Iterator, List

import boto3
from schemey import schema_from_type

from persisty.attr.attr import Attr
from persisty.attr.attr_filter_op import TYPE_FILTER_OPS
from persisty.attr.attr_type import attr_type, AttrType
from persisty.errors import PersistyError
from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.impl.dynamodb.partition_sort_index import PartitionSortIndex, from_schema
from persisty.impl.dynamodb.dynamodb_table_store import DynamodbTableStore
from persisty.index.attr_index import AttrIndex
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.key_config.composite_key_config import CompositeKeyConfig
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.security.restrict_access_store import restrict_access_store
from persisty.store.referential_integrity_store import ReferentialIntegrityStore
from persisty.store.schema_validating_store import SchemaValidatingStore
from persisty.store.store_abc import StoreABC
from persisty.store.unique_index_store import unique_index_store
from persisty.store_meta import StoreMeta
from persisty.util import filter_none


@dataclass
class DynamodbStoreFactory(StoreFactoryABC):
    aws_profile_name: Optional[str] = None
    region_name: Optional[str] = field(
        default_factory=lambda: os.environ.get("AWS_REGION") or "us-east-1"
    )
    table_name: Optional[str] = None
    index: Optional[PartitionSortIndex] = None
    global_secondary_indexes: Optional[Dict[str, PartitionSortIndex]] = None
    referential_integrity: bool = False

    def create(self, store_meta: StoreMeta) -> StoreABC:
        store = DynamodbTableStore(
            meta=store_meta,
            table_name=self.table_name,
            index=self.index,
            global_secondary_indexes=self.global_secondary_indexes or {},
            aws_profile_name=self.aws_profile_name,
            region_name=self.region_name,
        )
        store = SchemaValidatingStore(store)
        store = restrict_access_store(store, store_meta.store_access)
        store = unique_index_store(store)
        if self.referential_integrity:
            store = ReferentialIntegrityStore(store)
        return store

    def derive_from_meta(self, store_meta: StoreMeta):
        if self.table_name is None:
            self.table_name = store_meta.name
        if self.index is None:
            key_config = store_meta.key_config
            key_config_attrs = list(_get_attrs_from_key(key_config))
            self.index = PartitionSortIndex(*key_config_attrs)
        if self.global_secondary_indexes is None:
            self.global_secondary_indexes = {}
            for index in store_meta.indexes:
                if isinstance(index, AttrIndex):
                    self.global_secondary_indexes[
                        f"gix__{index.attr_name}"
                    ] = PartitionSortIndex(index.attr_name)
                elif isinstance(index, PartitionSortIndex):
                    if index.sk:
                        self.global_secondary_indexes[
                            f"gix__{index.pk}__{index.sk}"
                        ] = index
                    else:
                        self.global_secondary_indexes[f"gix__{index.pk}"] = index

    def get_session(self):
        kwargs = filter_none(
            {"profile_name": self.aws_profile_name, "region_name": self.region_name}
        )
        session = boto3.Session(**kwargs)
        return session

    def load_from_aws(self) -> StoreMeta:
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
        store_meta = StoreMeta(
            name=self.table_name,
            attrs=attrs,
            key_config=key_config,
        )
        return store_meta

    def create_table_in_aws(self, store_meta: StoreMeta):
        dynamodb = self.get_session().client("dynamodb")
        kwargs = {
            "AttributeDefinitions": self.get_attribute_definitions(store_meta),
            "TableName": self.table_name,
            "KeySchema": self.index.to_schema(),
            "BillingMode": "PAY_PER_REQUEST",  # Ops teams will want to look at these values
        }
        if self.global_secondary_indexes:
            kwargs["GlobalSecondaryIndexes"] = self.get_global_secondary_indexes()
        response = dynamodb.create_table(**kwargs)
        return response

    def get_attribute_definitions(self, store_meta: StoreMeta) -> List[Dict]:
        attrs = {}
        self._attrs(store_meta, self.index, attrs)
        if self.global_secondary_indexes:
            for index in self.global_secondary_indexes.values():
                self._attrs(store_meta, index, attrs)
        return list(attrs.values())

    def get_global_secondary_indexes(self):
        return [
            {
                "IndexName": k,
                "KeySchema": i.to_schema(),
                "Projection": {"ProjectionType": "ALL"},
            }
            for k, i in (self.global_secondary_indexes or {}).items()
        ]

    def _attrs(self, store_meta: StoreMeta, index: PartitionSortIndex, attrs: Dict):
        attrs[index.pk] = self._attr(store_meta, index.pk)
        if index.sk:
            attrs[index.sk] = self._attr(store_meta, index.sk)

    @staticmethod
    def _attr(store_meta: StoreMeta, name: str):
        attr = next(a for a in store_meta.attrs if a.name == name)
        return {
            "AttributeName": name,
            "AttributeType": _FIELD_TYPE_2_DYNAMODB[attr.attr_type],
        }


def _remove_index(indexed_attrs: Set[str], index: PartitionSortIndex):
    indexed_attrs.remove(index.pk)
    if index.sk:
        indexed_attrs.remove(index.sk)


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
        attr_type=db_type,
        schema=schema_from_type(python_type),
        permitted_filter_ops=TYPE_FILTER_OPS.get(db_type),
    )
    return attr


def _get_attrs_from_key(key_config: KeyConfigABC) -> Iterator[str]:
    if isinstance(key_config, AttrKeyConfig):
        yield key_config.attr_name
    elif isinstance(key_config, CompositeKeyConfig):
        for k in key_config.attrs:
            yield from _get_attrs_from_key(k)
    else:
        raise PersistyError("unsupported_key")
