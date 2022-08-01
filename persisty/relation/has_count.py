from dataclasses import dataclass

from marshy import ExternalType
from marshy.types import ExternalItemType

from persisty.access_control.authorization import Authorization
from persisty.field.field_filter import FieldFilterOp, FieldFilter
from persisty.relation.relation_abc import RelationABC, PersistyContext


@dataclass(frozen=True)
class HasCount(RelationABC):
    name: str
    storage_name: str
    id_field_name: str
    filter_attr_name: str

    def get_name(self):
        return self.name

    def resolve_for(
        self,
        item: ExternalItemType,
        context: PersistyContext,
        authorization: Authorization,
    ):
        key = item.get(self.id_field_name)
        if not key:
            return
        storage = context.get_storage(self.storage_name, authorization)
        count = storage.count(FieldFilter(self.filter_attr_name, FieldFilterOp.eq, key))
        item[self.name] = count
