from abc import ABC
from dataclasses import dataclass
from typing import Optional, ForwardRef, Union, Type

import typing_inspect
from servey.security.authorization import Authorization

from persisty.finder.stored_finder_abc import find_stored
from persisty.link.link_abc import LinkABC
from persisty.store_meta import StoreMeta
from persisty.util import to_snake_case


@dataclass
class LinkedStoreABC(LinkABC, ABC):
    linked_store_type: Union[Type, str, ForwardRef, None] = None
    name: Optional[str] = None  # Allows None so __set_name__ can exist
    linked_store_name: Optional[str] = None
    linked_store_meta: Optional[StoreMeta] = None

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

    def get_linked_store_meta(self):
        linked_store_meta = self.linked_store_meta
        if not linked_store_meta:
            linked_store_meta = next(
                meta
                for meta in find_stored()
                if meta.name == self.get_linked_store_name()
            )
            self.linked_store_meta = linked_store_meta
        return linked_store_meta

    def get_linked_store(self, authorization: Optional[Authorization] = None):
        meta = self.get_linked_store_meta()
        store = meta.store_factory.create(meta)
        store = meta.store_security.get_secured(store, authorization)
        return store
