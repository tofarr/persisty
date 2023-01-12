from dataclasses import dataclass
from typing import Optional, Callable

from servey.action.action import action
from servey.security.authorization import Authorization

from persisty.errors import PersistyError
from persisty.attr.attr_filter import AttrFilter, AttrFilterOp
from persisty.finder.store_factory_finder_abc import find_secured_store_factories
from persisty.link.link_abc import LinkABC
from persisty.util import to_snake_case


@dataclass
class HasCount(LinkABC):
    name: Optional[str] = None  # Allows None so __set_name__ can exist
    linked_store_name: Optional[str] = None
    key_attr_name: Optional[str] = None

    def __set_name__(self, owner, name):
        self.name = name
        if not self.linked_store_name:
            if name.endswith("_count"):
                self.linked_store_name = name[:-6]
            else:
                raise PersistyError(f"Please specify store name for: {name}")
        if self.key_attr_name is None:
            self.key_attr_name = f"{to_snake_case(owner.__name__)}_id"

    def get_name(self) -> str:
        return self.name

    def to_action_fn(self, owner_name: str) -> Callable:
        linked_store_factory = next(
            f
            for f in find_secured_store_factories()
            if f.get_meta().name == self.linked_store_name
        )
        key_attr_name = self.key_attr_name

        @action(name=owner_name + "_" + self.name)
        def action_fn(self, authorization: Optional[Authorization]) -> int:
            store = linked_store_factory.create(authorization)
            key = store.get_meta().key_config.to_key_str(self)
            count = store.count(
                search_filter=AttrFilter(key_attr_name, AttrFilterOp.eq, key)
            )
            return count

        action_fn.__name__ = owner_name + "_" + self.name
        return action_fn
