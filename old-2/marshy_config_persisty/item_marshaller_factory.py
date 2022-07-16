import dataclasses
from typing import Type, Optional, List

from marshy import marshaller_context
from marshy.factory.marshaller_factory_abc import MarshallerFactoryABC
from marshy.marshaller import marshaller_abc
from marshy.marshaller.deferred_marshaller import DeferredMarshaller
from marshy.marshaller.obj_marshaller import attr_config, ObjMarshaller

from persisty.util.undefined import UNDEFINED


@dataclasses.dataclass
class ItemMarshallerFactory(MarshallerFactoryABC):
    """
    Marshallers for items are pretty much the same as dataclasses, except they emit None values, and exclude
    UNDEFINED values.
    """
    priority: int = 110

    def create(self,
               context: marshaller_context.MarshallerContext,
               type_: Type) -> Optional[marshaller_abc.MarshallerABC]:
        if hasattr(type_, '__persisty_fields__'):
            attr_configs = get_attr_configs_for_type(type_, context)
            marshaller = ObjMarshaller[type_](type_, attr_configs)
            return marshaller


def get_attr_configs_for_type(type_: Type, context: marshaller_context.MarshallerContext):
    # noinspection PyDataclass
    fields: List[dataclasses.Field] = dataclasses.fields(type_)
    attr_configs = [attr_config(internal_name=f.name,
                                marshaller=DeferredMarshaller(f.type, context),
                                exclude_dumped_values=(UNDEFINED,))
                    for f in fields]
    return attr_configs
