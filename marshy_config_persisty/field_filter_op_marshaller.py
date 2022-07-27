from marshy.marshaller.marshaller_abc import MarshallerABC

from persisty.storage.field.field_filter import FieldFilterOp


class FieldFilterOpMarshaller(MarshallerABC[FieldFilterOp]):
    def __init__(self):
        super().__init__(FieldFilterOp)

    def load(self, item: str) -> FieldFilterOp:
        return FieldFilterOp[item]

    def dump(self, item: FieldFilterOp) -> str:
        return item.name
