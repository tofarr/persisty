from schemey import SchemaContext

from schemey_config_persisty_data.bytes_schema_factory import BytesSchemaFactory

priority = 110


def configure(context: SchemaContext):
    context.register_factory(BytesSchemaFactory())
