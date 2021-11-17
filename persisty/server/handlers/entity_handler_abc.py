from abc import ABC
from dataclasses import dataclass
from typing import Optional

from marshy.marshaller_context import MarshallerContext

from persisty.obj_graph.selection_set import SelectionSet, from_selection_set_list
from old.persisty.persisty_context import PersistyContext
from persisty.server.handlers.handler_abc import HandlerABC
from persisty.server.request import Request


@dataclass(frozen=True)
class EntityHandlerABC(HandlerABC, ABC):
    persisty_context: PersistyContext
    marshaller_context: MarshallerContext

    def get_entity_type(self, request: Request):
        entity_type_name = request.path[0]
        if self.persisty_context.has_entity(entity_type_name):
            return self.persisty_context.get_entity(entity_type_name)

    @staticmethod
    def get_selections(request: Request) -> Optional[SelectionSet]:
        selections_str = request.params.get('selections')
        if not selections_str:
            return
        selections_list = selections_str.split('~')
        selections = from_selection_set_list(selections_list)
        return selections
