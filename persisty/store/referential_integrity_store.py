from dataclasses import dataclass
from typing import Generic, List, Optional

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.errors import PersistyError
from persisty.finder.store_meta_finder_abc import find_store_meta
from persisty.link.inbound_link import InboundLink
from persisty.link.linked_store_abc import LinkedStoreABC
from persisty.link.on_delete import OnDelete
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store.filtered_store_abc import FilteredStoreABC, T
from persisty.store.store_abc import StoreABC


@dataclass
class ReferentialIntegrityStore(FilteredStoreABC[T], Generic[T]):
    """
    Store which maintains belongs to links, and prevents deletion of items.
    Useful for maintaining referential integrity in cases where the underlying
    storage mechanism does not support it. (Dynamodb / Mem)
    """

    store: StoreABC[T]
    blocking_links: Optional[List[InboundLink]] = None
    nullifying_links: Optional[List[InboundLink]] = None
    cascading_links: Optional[List[InboundLink]] = None

    def get_store(self) -> StoreABC[T]:
        return self.store

    def delete(self, key: str) -> bool:
        if self.block_delete(key):
            raise PersistyError("link_constraint_violated")
        result = self.get_store().delete(key)
        if result:
            self.nullify(key)
            self.cascade(key)
        return result

    def _delete(self, key: str, item: T) -> bool:
        if self.block_delete(key):
            raise PersistyError("link_constraint_violated")
        # pylint: disable=W0212
        result = self.get_store()._delete(key, item)
        if result:
            self.nullify(key)
            self.cascade(key)
        return result

    def block_delete(self, key: str) -> bool:
        for inbound_link in self.get_blocking_links():
            store = inbound_link.store_meta.create_store()
            search_filter = AttrFilter(inbound_link.attr_name, AttrFilterOp.eq, key)
            result_set = store.search(search_filter)
            if result_set.results:
                return True
        return False

    def nullify(self, key: str):
        count = 0
        for inbound_link in self.get_nullifying_links():
            store = inbound_link.store_meta.create_store()
            edits = self._get_nullify_updates(store, inbound_link.attr_name, key)
            count += sum(1 for _ in store.edit_all(edits))
        return count

    @staticmethod
    def _get_nullify_updates(store: StoreABC, attr_name: str, key: str):
        search_filter = AttrFilter(attr_name, AttrFilterOp.eq, key)
        items = store.search_all(search_filter)
        for item in items:
            setattr(item, attr_name, None)
            edit = BatchEdit(update_item=item)
            yield edit

    def cascade(self, key: str):
        count = 0
        for inbound_link in self.get_cascading_links():
            store = inbound_link.store_meta.create_store()
            edits = self._get_cascade_updates(store, inbound_link.attr_name, key)
            count += sum(1 for _ in store.edit_all(edits))
        return count

    @staticmethod
    def _get_cascade_updates(store: StoreABC, attr_name: str, key: str):
        key_config = store.get_meta().key_config
        search_filter = AttrFilter(attr_name, AttrFilterOp.eq, key)
        items = store.search_all(search_filter)
        for item in items:
            item_key = key_config.to_key_str(item)
            edit = BatchEdit(delete_key=item_key)
            yield edit

    def get_blocking_links(self):
        if self.blocking_links is None:
            self.populate_links()
        return self.blocking_links

    def get_cascading_links(self):
        if self.cascading_links is None:
            self.populate_links()
        return self.cascading_links

    def get_nullifying_links(self):
        if self.nullifying_links is None:
            self.populate_links()
        return self.nullifying_links

    def populate_links(self):
        inbound_links = get_inbound_links(self)
        self.blocking_links = [
            link for link in inbound_links if link.on_delete == OnDelete.BLOCK
        ]
        self.nullifying_links = [
            link for link in inbound_links if link.on_delete == OnDelete.NULLIFY
        ]
        self.cascading_links = [
            link for link in inbound_links if link.on_delete == OnDelete.CASCADE
        ]

    def delete_all(self, search_filter: SearchFilterABC[T]):
        if (
            self.get_blocking_links()
            or self.get_cascading_links()
            or self.get_nullifying_links()
        ):
            StoreABC.delete_all(self, search_filter)
        else:
            self.get_store().delete_all(search_filter)


def get_inbound_links(store: StoreABC) -> List[InboundLink]:
    inbound_links = []
    name = store.get_meta().name
    for store_meta in find_store_meta():
        for link in store_meta.links:
            if not isinstance(link, LinkedStoreABC):
                continue
            linked_store_name = link.get_linked_store_name()
            if linked_store_name != name:
                continue
            for inbound_link in link.get_inbound_links(store_meta):
                if inbound_link.on_delete == OnDelete.IGNORE:
                    continue
                inbound_links.append(inbound_link)
    return inbound_links
