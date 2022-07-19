from dataclasses import dataclass
from typing import Optional

from marshy.marshaller.marshaller_abc import MarshallerABC

from persisty.obj_storage.obj_storage_abc import T, F, C, U
from persisty.access_control.access_control_abc import AccessControlABC
from persisty.access_control.obj_access_control_abc import ObjAccessControlABC
from persisty.search_filter import INCLUDE_ALL


@dataclass(frozen=True)
class ObjAccessControl(ObjAccessControlABC[T]):
    access_control: AccessControlABC
    item_marshaller: MarshallerABC[T]
    create_input_marshaller: MarshallerABC[C]
    update_input_marshaller: MarshallerABC[U]

    def is_creatable(self, item: C) -> bool:
        item = self.create_input_marshaller.dump(item)
        return self.access_control.is_creatable(item)

    def is_readable(self, item: T) -> bool:
        item = self.item_marshaller.dump(item)
        return self.access_control.is_readable(item)

    def is_updatable(self, old_item: T, updates: U) -> bool:
        old_item = self.item_marshaller.dump(old_item)
        updates = self.update_input_marshaller.dump(updates)
        return self.access_control.is_updatable(old_item, updates)

    def is_deletable(self, item: T) -> bool:
        item = self.item_marshaller.dump(item)
        return self.access_control.is_deletable(item)

    def is_searchable(self) -> bool:
        return self.access_control.is_searchable()

    def transform_search_filter(
        self, search_filter_factory: Optional[F]
    ) -> Optional[F, bool]:
        search_filter = (
            search_filter_factory.to_search_filter()
            if search_filter_factory
            else INCLUDE_ALL
        )
        search_filter, handled = self.access_control.transform_search_filter(
            search_filter
        )
        # This works because all search filters are immutable search filter factories (returning self)
        return search_filter, handled
