from typing import Type, Optional, List

import typing_inspect
from marshy.factory.marshaller_factory_abc import MarshallerFactoryABC
from marshy.marshaller import str_marshaller
from marshy.marshaller.iterable_marshaller import IterableMarshaller
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller.obj_marshaller import ObjMarshaller, AttrConfig
from marshy.marshaller.optional_marshaller import OptionalMarshaller
from marshy.marshaller_context import MarshallerContext

from persisty.page import Page


class PageMarshallerFactory(MarshallerFactoryABC):
    priority: int = 200

    def create(self, context: MarshallerContext, type_: Type) -> Optional[MarshallerABC]:
        origin = typing_inspect.get_origin(type_)
        if origin is Page:
            item_type = typing_inspect.get_args(type_)[0]
            item_marshaller = context.get_marshaller(item_type)
            return ObjMarshaller(Page, (
                AttrConfig('items', 'items', IterableMarshaller(List[item_type], item_marshaller), True, True),
                AttrConfig('next_page_key', 'next_page_key', OptionalMarshaller(str_marshaller), True, True)
            ))
