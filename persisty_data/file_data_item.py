import io
import mimetypes
import os
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional

from persisty.util import UNDEFINED
from persisty_data.data_item_abc import DataItemABC
from persisty_data.data_store_abc import calculate_etag


@dataclass
class FileDataItem(DataItemABC):
    path: Path
    key: str = UNDEFINED
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
            with self.get_data_reader() as reader:
                etag = calculate_etag(reader)
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
