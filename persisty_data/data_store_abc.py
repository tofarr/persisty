import hashlib
from abc import ABC, abstractmethod
from typing import Iterator, Optional

from persisty.finder.store_finder_abc import find_stores
from persisty.store.store_abc import StoreABC
from persisty_data.data_item_abc import DataItemABC


class DataStoreABC(StoreABC[DataItemABC], ABC):

    @abstractmethod
    def get_data_writer(self, key: str, content_type: Optional[str] = None):
        pass

    def copy_data_from(self, source: DataItemABC):
        """
        Copy the data from the item given into this data store - implementions may use OS features to speed this up
        """
        with source.get_data_reader() as reader:
            with self.get_data_writer(source.key, source.content_type) as writer:
                copy_data(reader, writer)


def find_data_stores() -> Iterator[DataStoreABC]:
    yield from (s for s in find_stores() if isinstance(s, DataStoreABC))


def copy_data(reader, writer, buffer_size: int = 64 * 1024):
    while True:
        buffer = reader.read(buffer_size)
        if not buffer:
            return
        writer.write(buffer)


def calculate_etag(reader, buffer_size: int = 64 * 1024) -> str:
    md5 = hashlib.md5()
    while True:
        bytes_ = reader.read(buffer_size)
        if not bytes_:
            break
        md5.update(bytes_)
    result = md5.hexdigest()
    return result
