from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Tuple
from uuid import uuid4

from servey.cache_control.cache_control_abc import CacheControlABC

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.data_storage.data_item import DataItem
from persisty.data_storage.data_meta import DataMetaFilter, DataMeta
from persisty.data_storage.data_storage_abc import DataStorageABC
from persisty.errors import PersistyError
from persisty.storage.result_set import ResultSet


@dataclass
class DirectoryDataStorage(DataStorageABC):
    """
    Data storage where a file system directory is used to store files. Not really tenable in a lambda environment.
    Works in conjunction with route factories for data storage. In an AWS lambda environment, you would almost
    certainly use the S3 implementation instead of this.
    """

    name: str
    directory_path: str
    access_control: AccessControlABC
    cache_control: CacheControlABC
    root_url_pattern: str
    max_chunk_size: int = 1024 * 1024 * 10  # Maybe this needs adjusting?
    max_upload_age: int = 3600

    def get_name(self) -> str:
        return self.name

    def get_access_control(self) -> AccessControlABC:
        return self.access_control

    def get_cache_control(self) -> CacheControlABC:
        return self.cache_control

    def get_max_chunk_size(self) -> int:
        return self.max_chunk_size

    def get_max_upload_age(self) -> int:
        return self.max_upload_age

    def url_for_create(self, key: Optional[str] = None) -> str:
        if key:
            _check_key(key)
        else:
            key = str(uuid4())
        return self.root_url_pattern.format(key=key)

    def begin_upload(self, key: Optional[str] = None) -> str:
        if key:
            _check_key(key)
        else:
            key = str(uuid4())
        upload_id = key + "/" + str(uuid4())
        path = Path(self.directory_path, "uploads", upload_id)
        path.mkdir(parents=True)
        return upload_id

    def upload_chunk(self, upload_id: str, part_number: int, data: bytes):
        _check_key(upload_id)
        path = Path(self.directory_path, "uploads", upload_id, str(part_number))
        with open(path, "wb") as f:
            f.write(data)

    def complete_upload(
        self, upload_id, part_number: Optional[int] = None, data: Optional[bytes] = None
    ) -> str:
        _check_key(upload_id)
        input_path = Path(self.directory_path, "uploads", upload_id)
        output_path = Path(self.directory_path, "storage", key)
        zzzzz

    def abort_upload(self, upload_id: str) -> bool:
        pass

    def delete(self, key: str) -> bool:
        pass

    def url_for_read(self, key: Optional[str] = None) -> str:
        pass

    def read_meta(self, key: str) -> DataMeta:
        pass

    def read(self, key: str, data_range: Optional[Tuple[int, int]] = None) -> DataItem:
        pass

    def search(
        self,
        search_filter: Optional[DataMetaFilter] = None,
        page_key: Optional[str] = None,
        limit: int = 100,
    ) -> ResultSet[DataMeta]:
        pass


"""
    def get_signed_url_for_put(self, key: Optional[str] = None, exp: int = 3600) -> str:
        path = encrypt(dict(op='put', key=key, exp=exp))
        result = self.root_url + path
        return result

    def put(self, data: IO, key: Optional[str] = None, mime_type: Optional[str] = None) -> DataItem:
        if key:
            _check_key(key)
        else:
            key = str(uuid4())
        path = Path(self.directory_path, key)
        path.parent.mkdir(parents=True)
        with open(path, 'wb') as f:
            while True:
                b = data.read(self.buffer_size)
                if not b:
                    break
                f.write(b)
        return self.read(key)

    def get_url_for_read(self, key: str, exp: Optional[int] = None) -> str:
        path = encrypt(dict(op='read', key=key, exp=exp))
        result = self.root_url + path
        return result

    def open_for_read(self, key: str) -> Optional[IO]:
        _check_key(key)
        path = Path(self.directory_path, key)
        if path.exists():
            return open(path, 'rb')

    def read(self, key: str) -> Optional[DataItem]:
        _check_key(key)
        path = Path(self.directory_path, key)
        if path.exists():
            stat = path.stat()
            result = DataItem(
                key=key,
                size_in_bytes=stat.st_size,
                updated_at=datetime.fromtimestamp(stat.st_mtime),
                mime_type=mimetypes.guess_type(key)[0]
            )
            return result

    def delete(self, key: str) -> bool:
        _check_key(key)
        path = Path(self.directory_path, key)
        if not path.exists():
            return False
        os.remove(path)
        return True

    def search_all(self, search_filter: Optional[DataItemFilter] = None) -> Iterator[DataItem]:
        search_filter = search_filter.to_search_filter() if search_filter else INCLUDE_ALL
        dir_list = os.listdir(self.directory_path)
        attrs = get_storage_meta(DataItem).attrs
        for key in dir_list:
            path = Path(self.directory_path, key)
            stat = path.stat()
            result = DataItem(
                key=key,
                size_in_bytes=stat.st_size,
                updated_at=datetime.fromtimestamp(stat.st_mtime),
                mime_type=mimetypes.guess_type(key)[0]
            )
            if search_filter is INCLUDE_ALL:
                yield result
            else:
                item = dump(result)
                if search_filter.match(item, attrs):
                    yield result

    def search(
        self,
        search_filter: Optional[DataItemFilter] = None,
        page_key: Optional[str] = None,
        limit: int = 100
    ) -> ResultSet[DataItem]:
        items = self.search_all(search_filter)
        if page_key:
            last_item = next((item for item in items if item.key == page_key), None)
            if last_item is None:
                raise PersistyError('invalid_page_key')
            results = list(islice(items, 100))
            return ResultSet(
                results=results,
                next_page_key=results[-1].key if results else None
            )
"""


def _check_key(key: str):
    if ".." in key or "/" in key or "\\" in key or ":" in key:
        raise PersistyError("invalid_key")
