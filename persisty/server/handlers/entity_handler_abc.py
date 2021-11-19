from abc import ABC
from dataclasses import dataclass
from typing import Optional

from marshy.marshaller_context import MarshallerContext

from persisty.entity.entity_context import EntityContext
from persisty.entity.selections import Selections, from_selection_set_list
from persisty.server.handlers.handler_abc import HandlerABC
from persisty.server.request import Request


@dataclass(frozen=True)
class EntityHandlerABC(HandlerABC, ABC):
    entity_context: EntityContext
    marshaller_context: MarshallerContext

    def get_entity_type(self, request: Request):
        entity_type_name = request.path[0]
        if self.entity_context.has_entity(entity_type_name):
            return self.entity_context.get_entity(entity_type_name)

    @staticmethod
    def get_selections(request: Request) -> Optional[Selections]:
        selections_str = request.params.get('selections')
        if not selections_str:
            return
        selections_list = selections_str.split('~')
        selections = from_selection_set_list(selections_list)
        return selections
