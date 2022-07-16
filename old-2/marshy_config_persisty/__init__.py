from marshy.marshaller_context import MarshallerContext

from marshy_config_persisty.item_marshaller_factory import ItemMarshallerFactory

priority = 100


def configure(context: MarshallerContext):
    # Generic types require special marshallers...
    context.register_factory(ItemMarshallerFactory())
