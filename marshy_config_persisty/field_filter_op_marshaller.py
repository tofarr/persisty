from marshy.marshaller.marshaller_abc import MarshallerABC

from persisty.attr.attr_filter import AttrFilterOp


class FieldFilterOpMarshaller(MarshallerABC[AttrFilterOp]):
    def __init__(self):
        super().__init__(AttrFilterOp)

    def load(self, item: str) -> AttrFilterOp:
        return AttrFilterOp[item]

    def dump(self, item: AttrFilterOp) -> str:
        return item.name
