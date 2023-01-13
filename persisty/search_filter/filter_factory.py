from typing import Type
from dataclasses import dataclass

from persisty.attr.attr import Attr
from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.errors import PersistyError
from persisty.search_order.search_order import SearchOrder

from persisty.search_order.search_order_attr import SearchOrderAttr
from persisty.store_meta import get_meta, StoreMeta


@dataclass
class ObjFilterFactory:
    meta: StoreMeta

    def __getattr__(self, name):
        try:
            attr = next(a for a in self.meta.attrs if a.name == name and a.readable)
            return AttrFilterFactory(attr)
        except StopIteration:
            raise PersistyError(f"no_such_field:{name}")


@dataclass
class AttrFilterFactory:
    attr: Attr

    def asc(self) -> SearchOrder:
        return SearchOrder((SearchOrderAttr(self.attr.name),))

    def desc(self):
        return SearchOrder((SearchOrderAttr(self.attr.name, True),))

    def __getattr__(self, name):
        op = next(op for op in self.attr.permitted_filter_ops if op.name == name)
        return AttrOpFilterFactory(self.attr, op)


@dataclass
class AttrOpFilterFactory:
    attr: Attr
    op: AttrFilterOp

    def __call__(self, value):
        return AttrFilter(self.attr.name, self.op, value)


def filter_factory(type_: Type) -> ObjFilterFactory:
    return ObjFilterFactory(get_meta(type_))
