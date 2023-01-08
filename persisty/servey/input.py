import dataclasses
import os
from dataclasses import dataclass
from typing import Dict, Type, Optional

from marshy.factory.dataclass_marshaller_factory import DataclassMarshallerFactory
from marshy.marshaller_context import MarshallerContext
from schemey import SchemaContext, Schema
from schemey.factory.dataclass_schema_factory import DataclassSchemaFactory

from persisty.field.write_transform.write_transform_mode import WriteTransformMode
from persisty.storage.storage_meta import StorageMeta


class _PersistyMissing:
    pass


PERSISTY_MISSING = _PersistyMissing()


def _default_factory():
    return PERSISTY_MISSING


class Input:
    """Marker interface for inputs - We customize the way we generate schemas."""

    @classmethod
    def __schema_factory__(
        cls, context: SchemaContext, path: str, ref_schemas: Dict[Type, Schema]
    ):
        factory = DataclassSchemaFactory()
        schema = factory.from_type(cls, context, path, ref_schemas)
        schema.schema["additional_properties"] = (
            os.environ.get("SERVEY_STRICT_SCHEMA") != "1"
        )
        # for prop in schema.schema['properties'].values():
        #    prop.pop('default', None)
        return schema

    @classmethod
    def __marshaller_factory__(cls, marshaller_context: MarshallerContext):
        factory = DataclassMarshallerFactory(exclude_dumped_values=(PERSISTY_MISSING,))
        marshaller = factory.create(marshaller_context, cls)
        return marshaller


def input_type_for_create(storage_meta: StorageMeta):
    annotations = {}
    params = {
        "__doc__": f"Item for {storage_meta.name} create",
        "__annotations__": annotations,
    }
    default_params = {}
    default_annotations = {}
    for field in storage_meta.fields:
        mode = WriteTransformMode.SPECIFIED
        if field.write_transform:
            mode = field.write_transform.get_create_mode()
        if mode == WriteTransformMode.GENERATED:
            continue  # Field does not appear as part of create operations
        param_type = field.schema.python_type
        param_field = dataclasses.field(metadata=dict(schemey=field.schema))
        if mode == WriteTransformMode.OPTIONAL:
            param_type = Optional[param_type]
            param_field.default_factory = _default_factory
            default_params[field.name] = param_field
            default_annotations[field.name] = param_type
        else:
            params[field.name] = param_field
            annotations[field.name] = param_type
    params.update(**default_params)  # Make sure ordering is correct
    annotations.update(**default_annotations)
    type_ = dataclass(type(f"{storage_meta.name}Create", (Input,), params))
    return type_


def input_type_for_update(storage_meta: StorageMeta):
    annotations = {}
    params = {
        "__doc__": f"Item for {storage_meta.name} update",
        "__annotations__": annotations,
    }
    default_params = {}
    default_annotations = {}
    for field in storage_meta.fields:
        mode = WriteTransformMode.OPTIONAL
        if field.write_transform:
            mode = field.write_transform.get_update_mode()
        if (
            mode == WriteTransformMode.GENERATED
            or storage_meta.key_config.is_required_field(field.name)
        ):
            continue  # Field does not appear as part of create operations
        param_type = field.schema.python_type
        param_field = dataclasses.field(metadata=dict(schemey=field.schema))
        if mode == WriteTransformMode.OPTIONAL:
            default_annotations[field.name] = Optional[param_type]
            default_params[field.name] = param_field
        else:
            annotations[field.name] = param_type
            params[field.name] = param_field

    params.update(**default_params)  # Make sure ordering is correct
    annotations.update(**default_annotations)
    type_ = dataclass(type(f"{storage_meta.name}Update", (Input,), params))
    return type_
