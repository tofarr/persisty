from abc import ABC
from dataclasses import dataclass
from typing import Optional, ForwardRef, Union, Type

import typing_inspect
from servey.security.authorization import Authorization

from persisty.finder.store_finder_abc import find_store_factories
from persisty.link.link_abc import LinkABC
from persisty.util import to_snake_case


@dataclass
class LinkedStoreABC(LinkABC, ABC):
    linked_store_type: Union[Type, str, ForwardRef]
    name: Optional[str] = None  # Allows None so __set_name__ can exist
    linked_store_name: Optional[str] = None

    def get_linked_store_name(self):
        linked_store_name = self.linked_store_name
        if not linked_store_name:
            linked_type = self.linked_store_type
            if isinstance(linked_type, ForwardRef):
                linked_type = typing_inspect.get_forward_arg(linked_type).split(".")[-1]
            elif isinstance(linked_type, type):
                linked_type = linked_type.__name__
            linked_store_name = to_snake_case(linked_type)
        return linked_store_name

    def get_linked_store_factory(self):
        linked_store_factory = getattr(self, "_linked_store_factory", None)
        if not linked_store_factory:
            linked_store_factory = next(
                f
                for f in find_store_factories()
                if f.get_meta().name == self.get_linked_store_name()
            )
            setattr(self, "_linked_store_factory", linked_store_factory)
        return linked_store_factory

    def get_linked_store(self, authorization: Optional[Authorization] = None):
        store = self.get_linked_store_factory().create(authorization)
        return store
