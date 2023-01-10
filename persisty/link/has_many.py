from dataclasses import dataclass
from typing import Optional

from marshy import get_default_context
from servey.action.action import action
from servey.security.authorization import Authorization

from persisty.errors import PersistyError
from persisty.field.field_filter import FieldFilter, FieldFilterOp
from persisty.finder.storage_factory_finder_abc import find_secured_storage_factories
from persisty.link.link_abc import LinkABC
from persisty.servey import get_item_type, get_result_set_type
from persisty.util import to_snake_case


@dataclass
class HasMany(LinkABC):
    name: Optional[str] = None  # Allows None so __set_name__ can exist
    linked_storage_name: Optional[str] = None
    key_field_name: Optional[str] = None

    def __set_name__(self, owner, name):
        self.name = name
        if not self.linked_storage_name:
            if name.endswith("_result_set"):
                self.linked_storage_name = name[:-11]
            else:
                raise PersistyError(f"Please specify storage name for: {name}")
        if self.key_field_name is None:
            self.key_field_name = f"{to_snake_case(owner.__name__)}_id"

    def get_name(self) -> str:
        return self.name

    def to_action_fn(self, owner_name: str):
        linked_storage_factory = next(
            f
            for f in find_secured_storage_factories()
            if f.get_storage_meta().name == self.linked_storage_name
        )
        key_field_name = self.key_field_name
        item_type = get_item_type(linked_storage_factory.get_storage_meta())
        marshaller = get_default_context().get_marshaller(item_type)
        result_set_type = get_result_set_type(item_type)

        @action(name=owner_name + "_" + self.name)
        def action_fn(self, authorization: Optional[Authorization]) -> result_set_type:
            storage = linked_storage_factory.create(authorization)
            key = storage.get_storage_meta().key_config.to_key_str(self)
            result_set = storage.search(
                search_filter=FieldFilter(key_field_name, FieldFilterOp.eq, key)
            )
            result_set = result_set_type(
                results=[marshaller.load(r) for r in result_set.results],
                next_page_key=result_set.next_page_key,
            )
            return result_set

        action_fn.__name__ = owner_name + "_" + self.name
        return action_fn
