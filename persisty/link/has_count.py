from dataclasses import dataclass
from typing import Optional, Callable

from servey.action.action import action
from servey.security.authorization import Authorization

from persisty.errors import PersistyError
from persisty.field.field_filter import FieldFilter, FieldFilterOp
from persisty.finder.storage_factory_finder_abc import find_storage_factories
from persisty.link.link_abc import LinkABC
from persisty.util import to_snake_case


@dataclass
class HasCount(LinkABC):
    name: Optional[str] = None  # Allows None so __set_name__ can exist
    linked_storage_name: Optional[str] = None
    key_field_name: Optional[str] = None

    def __set_name__(self, owner, name):
        self.name = name
        if not self.linked_storage_name:
            if name.endswith("_count"):
                self.linked_storage_name = name[:-6]
            else:
                raise PersistyError(f"Please specify storage name for: {name}")
        if self.key_field_name is None:
            self.key_field_name = f"{to_snake_case(owner.__name__)}_id"

    def get_name(self) -> str:
        return self.name

    def to_action_fn(self, owner_name: str) -> Callable:
        linked_storage_factory = next(
            f for f in find_storage_factories()
            if f.get_storage_meta().name == self.linked_storage_name
        )
        key_field_name = self.key_field_name

        @action(name=owner_name+'_'+self.name)
        def action_fn(self, authorization: Optional[Authorization]) -> int:
            storage = linked_storage_factory.create(authorization)
            key = storage.get_storage_meta().key_config.to_key_str(self)
            count = storage.count(search_filter=FieldFilter(key_field_name, FieldFilterOp.eq, key))
            return count

        action_fn.__name__ = owner_name+'_'+self.name
        return action_fn

