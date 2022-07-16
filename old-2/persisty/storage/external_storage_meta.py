from dataclasses import dataclass
from typing import Optional, Tuple, List, Iterator

from schemey.object_schema import ObjectSchema
from schemey.schema_abc import SchemaABC
from schemey.schema_context import schema_for_type

from persisty.item.field import Field
from persisty.item.generator.generator_mode import GeneratorMode
from persisty.key_config.attr_key_config import ATTR_KEY_CONFIG
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.security.access_control import ALL_ACCESS
from persisty.security.access_control_abc import AccessControlABC
from persisty.security.authorization import Authorization
from persisty.storage.storage_meta import StorageMeta


@dataclass
class ExternalStorageMeta:
    """ Storage meta in an external form useful to external clients. """
    name: str
    item_schema: SchemaABC
    create_input_schema: SchemaABC
    update_input_schema: SchemaABC
    search_filter_schema: SchemaABC
    search_order_schema: SchemaABC
    key_config: KeyConfigABC = ATTR_KEY_CONFIG
    access_control: AccessControlABC = ALL_ACCESS
    description: Optional[str] = None


def external_storage_meta(storage_meta: StorageMeta, authorization: Authorization):
    return ExternalStorageMeta(
        name=storage_meta.name,
        item_schema=schema_for_item(storage_meta, authorization),
        create_input_schema=schema_for_write(storage_meta, authorization, GeneratorMode.ALWAYS_FOR_CREATE),
        update_input_schema=schema_for_write(storage_meta, authorization, GeneratorMode.ALWAYS_FOR_UPDATE),
        search_filter_schema=schema_for_type(storage_meta.search_filter_type).json_schema,
        search_order_schema=schema_for_type(storage_meta.search_order_type).json_schema,
        key_config=storage_meta.key_config,
        access_control=storage_meta.access_control,
        description=storage_meta.description
    )


def _fields_for_write(fields: Tuple[Field, ...],
                      authorization: Authorization,
                      generator_mode: GeneratorMode
                      ) -> Iterator[Field]:
    for field in fields:
        if not field.access_control.is_writable(authorization):
            continue
        if field.generator:
            if field.generator.generator_mode in (generator_mode, GeneratorMode.ALWAYS_FOR_WRITE):
                continue
        yield field


def schema_for_item(storage_meta: StorageMeta, authorization: Authorization):
    fields = [f for f in storage_meta.fields if f.access_control.is_readable(authorization)]
    return ObjectSchema(
        properties={f.name: f.schema for f in fields},
        name=storage_meta.name,
        required={f.name for f in fields if f.generator is None},
        description=storage_meta.description
    )


def schema_for_write(storage_meta: StorageMeta, authorization: Authorization, generator_mode: GeneratorMode):
    fields = (f for f in storage_meta.fields if f.access_control.is_writable(authorization))
    fields = [f for f in fields
              if f.generator is None or f.generator.generator_mode not in (generator_mode,
                                                                           GeneratorMode.ALWAYS_FOR_WRITE)]
    return ObjectSchema(
        properties={f.name: f.schema for f in fields},
        name=storage_meta.name,
        required={f.name for f in fields if f.generator is None},
        description=storage_meta.description
    )
