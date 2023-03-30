from dataclasses import dataclass
from typing import Optional, Dict, Set, Iterator, List

import boto3
from schemey import schema_from_type

from persisty.attr.attr import Attr
from persisty.attr.attr_filter_op import TYPE_FILTER_OPS
from persisty.attr.attr_type import attr_type, AttrType
from persisty.errors import PersistyError
from persisty.impl.dynamodb.dynamodb_index import DynamodbIndex, from_schema
from persisty.impl.dynamodb.dynamodb_table_store import DynamodbTableStore
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.key_config.composite_key_config import CompositeKeyConfig
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.store.restrict_access_store import restrict_access_store
from persisty.store.schema_validating_store import SchemaValidatingStore
from persisty.store.store_abc import StoreABC
from persisty.store.unique_index_store import unique_index_store
from persisty.store_meta import StoreMeta
from persisty.util import filter_none


@dataclass
class DynamodbStoreFactory:
    meta: Optional[StoreMeta] = None
    aws_profile_name: Optional[str] = None
    region_name: Optional[str] = None
    table_name: Optional[str] = None
    index: Optional[DynamodbIndex] = None
    global_secondary_indexes: Optional[Dict[str, DynamodbIndex]] = None

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
        store = unique_index_store(store)
        return store

    def derive_from_meta(self):
        meta = self.meta
        if self.table_name is None:
            self.table_name = meta.name
        if self.index is None:
            key_config = meta.key_config
            key_config_attrs = list(_get_attrs_from_key(key_config))
            self.index = DynamodbIndex(*key_config_attrs)
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
        kwargs = dict(
            AttributeDefinitions=self.get_attribute_definitions(),
            TableName=self.table_name,
            KeySchema=self.index.to_schema(),
            BillingMode="PAY_PER_REQUEST",  # Ops teams will want to look at these values
        )
        if self.global_secondary_indexes:
            kwargs["GlobalSecondaryIndexes"] = self.get_global_secondary_indexes()
        response = dynamodb.create_table(**kwargs)
        return response

    def get_attribute_definitions(self) -> List[Dict]:
        attrs = {}
        self._attrs(self.index, attrs)
        if self.global_secondary_indexes:
            for index in self.global_secondary_indexes.values():
                self._attrs(index, attrs)
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

    def _attrs(self, index: DynamodbIndex, attrs: Dict):
        attrs[index.pk] = self._attr(index.pk)
        if index.sk:
            attrs[index.sk] = self._attr(index.sk)

    def _attr(self, name: str):
        attr = next(a for a in self.meta.attrs if a.name == name)
        return dict(
            AttributeName=name, AttributeType=_FIELD_TYPE_2_DYNAMODB[attr.attr_type]
        )


def _remove_index(indexed_attrs: Set[str], index: DynamodbIndex):
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
