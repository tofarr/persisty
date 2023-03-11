import dataclasses
import os.path
import shutil
from pathlib import Path
from typing import Optional, Iterator

from persisty.errors import PersistyError
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store_meta import T, StoreMeta
from persisty_data.data_item_abc import DataItemABC, DATA_ITEM_META
from persisty_data.data_store_abc import DataStoreABC
from persisty_data.file_data_item import FileDataItem


@dataclasses.dataclass
class DirectoryDataStore(DataStoreABC):
    """
    Data store backed by a directory
    """
    name: str
    directory: Path
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
        self.copy_data_from(item)
        return FileDataItem(path=path, key=item.key)

    def read(self, key: str) -> Optional[DataItemABC]:
        path = self._key_to_path(key)
        if os.path.isfile(path):
            return FileDataItem(path=path, key=key)

    def _update(self, key: str, item: FileDataItem, updates: DataItemABC) -> Optional[DataItemABC]:
        self.copy_data_from(updates)
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
                path = Path(root, file)
                key = str(Path(root, file))
                item = FileDataItem(path=path, key=key)
                if search_filter.match(item, meta.attrs):
                    yield item

    def _key_to_path(self, key: str):
        path = Path(self.directory, key)
        assert os.path.normpath(path) == str(path)  # Prevent ../ shenanigans
        return path

    def get_data_writer(self, key: str, content_type: Optional[str] = None):
        path = self._key_to_path(key)
        path.parent.mkdir(parents=True, exist_ok=True)
        return open(path, 'wb')

    def copy_data_from(self, source: DataItemABC):
        if not isinstance(source, FileDataItem):
            super().copy_data_from(source)
        if os.stat(source).st_size > self.max_item_size:
            raise PersistyError('max_item_size_exceeded')
        shutil.copyfile(source, self._key_to_path(source.key))


def directory_data_store(name: str):
    return DirectoryDataStore(
        name=name,
        directory=Path(name)
    )
