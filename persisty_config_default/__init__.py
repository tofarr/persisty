from marshy.factory.dataclass_marshaller_factory import DataclassMarshallerFactory

from persisty.context import PersistyContext
from persisty.util import UNDEFINED

priority = 50


def configure_context(persisty_context: PersistyContext):
    # We do this here to affect only the marshallers associated with schemey
    persisty_context.schema_context.marshaller_context.register_factory(
        DataclassMarshallerFactory(101, (UNDEFINED,))
    )
    return persisty_context
