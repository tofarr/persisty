import io
import mimetypes
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Union

from persisty.util import UNDEFINED
from persisty_data.data_item_abc import DataItemABC
from persisty_data.data_store_abc import calculate_etag


@dataclass
class MemDataItem(DataItemABC):
    value: Union[bytes, bytearray, type(None)]
    key: str
    data_url: Optional[str] = None
    updated_at: Optional[datetime] = None
    buffer_size: int = 1024 * 1024
    max_size: int = 1024 * 1024 * 10
    _etag: Optional[str] = UNDEFINED
    _content_type: Optional[str] = UNDEFINED

    @property
    def size(self) -> Optional[int]:
        return len(self.value)

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
            content_type = mimetypes.guess_type(self.key)[0]
            self._content_type = content_type
        return content_type

    def get_data_reader(self) -> io.IOBase:
        return io.BytesIO(self.value)
