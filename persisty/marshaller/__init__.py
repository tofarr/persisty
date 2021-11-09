from marshy.marshaller_context import MarshallerContext


def register_all_marshallers(context: MarshallerContext):
    from persisty.marshaller.edit_marshaller_factory import EditMarshallerFactory
    from persisty.marshaller.page_marshaller_factory import PageMarshallerFactory
    context.register_factory(EditMarshallerFactory())
    context.register_factory(PageMarshallerFactory())