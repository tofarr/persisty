from dataclasses import dataclass

from marshy import ExternalType
from marshy.types import ExternalItemType

from persisty.persisty_context import PersistyContext
from persisty.relation.relation_abc import RelationABC

@dataclass(frozen=True)
class BelongsTo(RelationABC):
    name: str
    storage_name: str
    id_field_name: str

    def get_name(self):
        return self.name

    def resolve_for(self, item: ExternalItemType, context: PersistyContext) -> ExternalType:
        id = item.get(self.id_field_name)
        if not id:
            return None
        context.
