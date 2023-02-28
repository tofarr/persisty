import io
import mimetypes
import os
import shutil
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

from persisty.errors import PersistyError
from persisty.util import UNDEFINED
from persisty_data_2.data_item_abc import DataItemABC, calculate_etag
from persisty_data_2.mem_data_item import MemDataItem


@dataclass
class FileDataItem(DataItemABC):
    path: Path
    key: str
    buffer_size: int = 1024 * 1024
    max_size: int = 1024 * 1024 * 100  # Default 100mb - seems fair
    _updated_at: Optional[datetime] = UNDEFINED
    _etag: Optional[str] = UNDEFINED
    _content_type: Optional[str] = UNDEFINED
    _size: Optional[int] = UNDEFINED

    @property
    def updated_at(self) -> Optional[datetime]:
        updated_at = self._updated_at
        if updated_at is not UNDEFINED:
            return updated_at
        try:
            updated_at = os.path.getmtime(self.path)
            updated_at = self._updated_at = datetime.fromtimestamp(updated_at)
            return updated_at
        except FileNotFoundError:
            pass

    @property
    def etag(self) -> Optional[str]:
        etag = self._etag
        if etag is not UNDEFINED:
            return etag
        try:
            etag = calculate_etag(self, self.buffer_size)
            self._etag = etag
            return etag
        except FileNotFoundError:
            pass

    @property
    def content_type(self) -> Optional[str]:
        content_type = self._content_type
        if content_type is UNDEFINED:
            content_type = mimetypes.guess_type(self.path)[0]
            self._content_type = content_type
        return content_type

    @property
    def size(self) -> Optional[int]:
        size = self._size
        if size is not UNDEFINED:
            return size
        try:
            self._size = size = os.path.getsize(self.path)
            return size
        except FileNotFoundError:
            self._size = None

    def get_data_reader(self) -> io.IOBase:
        return open(self.path, 'rb')

    def copy_data_to(self, destination):
        if isinstance(destination, str) or isinstance(destination, Path):
            Path(destination).parent.mkdir(parents=True, exist_ok=True)
            shutil.copyfile(self.path, destination)
        if isinstance(destination, bytearray):
            with open(self.path, 'rb') as reader:
                while True:
                    buffer = reader.read(self.buffer_size)
                    destination.extend(buffer)
                    if not buffer:
                        return
        elif isinstance(destination, DataItemABC):
            destination.copy_data_from(self.path)
        else:
            # Assume file like object
            with open(self.path, 'rb') as reader:
                while True:
                    buffer = reader.read(self.buffer_size)
                    # noinspection PyUnresolvedReferences
                    destination.write(buffer)
                    if not buffer:
                        return

    def copy_data_from(self, source):
        Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        if isinstance(source, str) or isinstance(source, Path):
            self._copy_data_from_path(source)
        elif isinstance(source, FileDataItem):
            self._copy_data_from_path(source.path)
        elif isinstance(source, bytes) or isinstance(source, bytearray):
            self._copy_data_from_bytes(source)
        elif isinstance(source, MemDataItem):
            self._copy_data_from_bytes(source.value)
        elif isinstance(source, DataItemABC):
            with open(self.path, 'wb') as writer:
                source.copy_data_to(writer)
                self._after_copy_data_from()
        else:
            # noinspection PyUnresolvedReferences
            with open(self.path, 'wb') as writer:
                size = 0
                while True:
                    buffer = source.read(self.buffer_size)
                    if not buffer:
                        self._after_copy_data_from()
                        return
                    size += len(buffer)
                    if size > self.max_size:
                        raise PersistyError('data_overflow')
                    writer.write(buffer)

    def _copy_data_from_path(self, source: Union[str, Path]):
        if os.stat(source).st_size > self.max_size:
            raise PersistyError('data_overflow')
        shutil.copyfile(source, self.path)
        self._after_copy_data_from()

    def _copy_data_from_bytes(self, source: Union[bytes, bytearray]):
        if len(source) > self.max_size:
            raise PersistyError('data_overflow')
        with open(self.path, 'wb') as writer:
            writer.write(source)
            self._after_copy_data_from()

    def _after_copy_data_from(self):
        self._etag = UNDEFINED
        self._updated_at = UNDEFINED
        self._size = UNDEFINED
