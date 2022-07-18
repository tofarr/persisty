from typing import Type, Optional

import typing_inspect
from marshy.factory.marshaller_factory_abc import MarshallerFactoryABC
from marshy.marshaller import str_marshaller
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller.obj_marshaller import ObjMarshaller, AttrConfig
from marshy.marshaller.optional_marshaller import OptionalMarshaller
from marshy.marshaller_context import MarshallerContext

from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.page import Page


class EditMarshallerFactory(MarshallerFactoryABC):
    priority: int = 200

    def create(self, context: MarshallerContext, type_: Type) -> Optional[MarshallerABC]:
        origin = typing_inspect.get_origin(type_)
        if origin is Edit:
            edit_type_marshaller = context.get_marshaller(EditType)
            key_marshaller = OptionalMarshaller(str_marshaller)
            item_type = typing_inspect.get_args(type_)[0]
            item_marshaller = OptionalMarshaller(context.get_marshaller(item_type))
            return ObjMarshaller(Page, (
                AttrConfig('edit_type', 'edit_type', edit_type_marshaller, True, True),
                AttrConfig('key', 'key', key_marshaller, True, True),
                AttrConfig('stored', 'stored', item_marshaller, True, True)
            ))
