from schemey import SchemaContext, Schema

from persisty.field.field_filter import FieldFilterOp

priority = 120


def configure(context: SchemaContext):
    context.schemas_by_type[FieldFilterOp] = Schema(
        {"name": FieldFilterOp.__name__, "enum": [f.name for f in FieldFilterOp]},
        FieldFilterOp,
    )
