from __future__ import annotations
from typing import Tuple

from dataclasses import dataclass, field

from marshy.types import ExternalItemType
from schemey import Schema, schema_from_json
from schemey.schema import str_schema

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.access_control.constants import ALL_ACCESS
from persisty.cache_control.cache_control_abc import CacheControlABC
from persisty.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty.key_config.field_key_config import FIELD_KEY_CONFIG
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.field.field import Field


@dataclass(frozen=True)
class StorageMeta:
    name: str = field(
        metadata=dict(schemey=str_schema(max_length=100, pattern="^\\w+$"))
    )
    fields: Tuple[Field, ...]
    key_config: KeyConfigABC = FIELD_KEY_CONFIG
    access_control: AccessControlABC = ALL_ACCESS
    cache_control: CacheControlABC = SecureHashCacheControl()
    batch_size: int = 100

    def to_schema(self) -> Schema:
        properties = {
            f.name: _add_prefix_to_refs(f.schema.schema, f"#{f.name}")
            for f in self.fields
        }
        schema = {
            "type": "object",
            "name": self.name,
            "properties": properties,
            "additionalProperties": False,
        }
        return schema_from_json(schema)


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
