from __future__ import annotations

from enum import Enum
from typing import Tuple, Optional, Callable

from dataclasses import dataclass, field

from marshy.types import ExternalItemType
from schemey import Schema, SchemaContext, get_default_schema_context
from schemey.schema import str_schema
from servey.cache_control.cache_control_abc import CacheControlABC
from servey.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty.key_config.field_key_config import FIELD_KEY_CONFIG
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.field.field import Field
from persisty.link.link_abc import LinkABC
from persisty.storage.storage_access import StorageAccess, ALL_ACCESS
from persisty.util import to_camel_case


def is_readable(field_: Field) -> bool:
    return field_.is_readable


@dataclass(frozen=True)
class StorageMeta:
    name: str = field(
        metadata=dict(schemey=str_schema(max_length=100, pattern="^\\w+$"))
    )
    fields: Tuple[Field, ...]
    key_config: KeyConfigABC = FIELD_KEY_CONFIG
    storage_access: StorageAccess = ALL_ACCESS
    cache_control: CacheControlABC = SecureHashCacheControl()
    batch_size: int = 100
    description: Optional[str] = None
    links: Tuple[LinkABC, ...] = tuple()

    def to_json_schema(
        self, prefix: str = "", check: Callable[[Field], bool] = is_readable
    ) -> ExternalItemType:
        properties = {
            f.name: _add_prefix_to_refs(
                f.schema.schema, f"#{prefix}properties/{f.name}"
            )
            for f in self.fields
            if check(f)
        }
        schema = {
            "type": "object",
            "name": self.name.title(),
            "properties": properties,
            "additionalProperties": False,
        }
        for link in self.links:
            link.update_json_schema(schema)
        return schema

    def schema_for_read(self):
        pass

    def schema_for_create(self):
        pass

    def schema_for_update(self):
        pass

    def to_schema(
        self,
        prefix: str = "",
        check: Callable[[Field], bool] = is_readable,
        schema_context: Optional[SchemaContext] = None,
    ) -> Schema:
        if not schema_context:
            schema_context = get_default_schema_context()
        schema = schema_context.schema_from_json(self.to_json_schema(prefix, check))
        return schema

    def get_sortable_fields_as_enum(self):
        fields = {f.name: f.name for f in self.fields if f.is_sortable}
        if fields:
            return Enum(f"{to_camel_case(self.name)}Sortable", fields)


def _add_prefix_to_refs(item: ExternalItemType, prefix: str):
    if isinstance(item, dict):
        ret = {k: _add_prefix_to_refs(v, prefix) for k, v in item.items()}
        ref = ret.get("$ref")
        if ref:
            ret["$ref"] = prefix + ref[1:]
        return ret
    elif isinstance(item, list):
        return [_add_prefix_to_refs(s, prefix) for s in item]
    return item
