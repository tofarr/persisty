from abc import ABC
from dataclasses import dataclass
from typing import Optional, ForwardRef, Union, Type, List

import typing_inspect
from servey.security.authorization import Authorization

from persisty.finder.store_meta_finder_abc import find_store_meta_by_name
from persisty.link.inbound_link import InboundLink
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
            linked_store_meta = find_store_meta_by_name(self.get_linked_store_name())
            self.linked_store_meta = linked_store_meta
        return linked_store_meta

    def get_linked_store(self, authorization: Optional[Authorization] = None):
        store_meta = self.get_linked_store_meta()
        store = store_meta.create_secured_store(authorization)
        return store

    # pylint: disable=W0613
    def get_inbound_links(self, store_meta: StoreMeta) -> List[InboundLink]:
        return []
