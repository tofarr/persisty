import dataclasses
import os.path
from pathlib import Path
from typing import Optional, Iterator

from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store.store_abc import StoreABC
from persisty.store_meta import T, StoreMeta
from persisty_data_2.data_item_abc import DataItemABC, DATA_ITEM_META
from persisty_data_2.file_data_item import FileDataItem
from persisty_data_2.web_data_interface_abc import WebDataInterfaceABC


class DirectoryDataStore(StoreABC[DataItemABC]):
    """
    Data store backed by a directory
    """
    name: str
    directory: Path
    buffer_size: int = 1024 * 1024
    web_data_interface: Optional[WebDataInterfaceABC] = None
    max_item_size: int = 1024 * 1024 * 100  # Default 100mb - seems fair
    _meta: StoreMeta = None

    def get_meta(self) -> StoreMeta:
        meta = self._meta
        if meta is None:
            meta = self._meta = dataclasses.replace(DATA_ITEM_META, name=self.name)
        return meta

    def create(self, item: DataItemABC) -> Optional[DataItemABC]:
        path = self._key_to_path(item.key)
        assert not os.path.exists(path)
        stored = FileDataItem(
            path=path,
            key=item.key
        )
        stored.copy_data_from(item)
        return stored

    def read(self, key: str) -> Optional[DataItemABC]:
        path = self._key_to_path(key)
        if os.path.isfile(path):
            return FileDataItem(
                path=path,
                key=key,
                buffer_size=self.buffer_size
            )

    def _update(self, key: str, item: FileDataItem, updates: DataItemABC) -> Optional[DataItemABC]:
        item.copy_data_from(updates)
        return item

    def _delete(self, key: str, item: FileDataItem) -> bool:
        item.path.unlink(True)
        return True

    def count(self, search_filter: SearchFilterABC[DataItemABC] = INCLUDE_ALL) -> int:
        result = sum(1, self.search_all(search_filter))
        return result

    def search_all(
        self,
        search_filter: SearchFilterABC[DataItemABC] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[DataItemABC]] = None
    ) -> Iterator[T]:
        items = self._search_all(search_filter)
        if search_order and search_order.orders:
            items = search_order.sort(items)
        yield from items

    def _search_all(self, search_filter: SearchFilterABC[DataItemABC]):
        meta = self.get_meta()
        for (root, _, files) in os.walk(self.directory):
            for file in files:
                path = Path(self.directory, root, file)
                key = str(Path(root, file))
                item = FileDataItem(path=path, key=key, max_size=self.max_item_size)
                if search_filter.match(item, meta.attrs):
                    yield item

    def _key_to_path(self, key: str):
        path = Path(self.directory, key)
        assert os.path.normpath(path) == str(path)  # Prevent ../ shenanigans
        return path
