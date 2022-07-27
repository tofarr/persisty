from typing import Type
from dataclasses import dataclass

from persisty.errors import PersistyError
from persisty.obj_storage.stored import get_storage_meta
from persisty.search_order.search_order import SearchOrder
from persisty.search_order.search_order_field import SearchOrderField
from persisty.storage.field.field import Field
from persisty.storage.field.field_filter import FieldFilterOp, FieldFilter
from persisty.storage.storage_meta import StorageMeta


@dataclass
class ObjFilterFactory:
    storage_meta: StorageMeta

    def __getattr__(self, name):
        try:
            field = next(
                field for field in self.storage_meta.fields if field.name == name
            )
            return FieldFilterFactory(field)
        except StopIteration:
            raise PersistyError(f"no_such_field:{name}")


@dataclass
class FieldFilterFactory:
    field: Field

    def asc(self) -> SearchOrder:
        return SearchOrder((SearchOrderField(self.field.name),))

    def desc(self):
        return SearchOrder((SearchOrderField(self.field.name, True),))

    def __getattr__(self, name):
        op = next(op for op in self.field.permitted_filter_ops if op.name == name)
        return FieldOpFilterFactory(self.field, op)


@dataclass
class FieldOpFilterFactory:
    field: Field
    op: FieldFilterOp

    def __call__(self, value):
        return FieldFilter(self.field.name, self.op, value)


def filter_factory(type_: Type) -> ObjFilterFactory:
    return ObjFilterFactory(get_storage_meta(type_))
