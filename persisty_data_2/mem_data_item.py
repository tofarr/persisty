import io
import mimetypes
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Union

from persisty.errors import PersistyError
from persisty.util import UNDEFINED
from persisty_data_2.data_item_abc import DataItemABC, calculate_etag


@dataclass
class MemDataItem(DataItemABC):
    value: Union[bytes, bytearray, type(None)]
    key: str
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

    def copy_data_to(self, destination):
        if isinstance(destination, str) or isinstance(destination, Path):
            Path(destination).parent.mkdir(parents=True, exist_ok=True)
            with open(destination, 'wb') as writer:
                writer.write(self.value)
        if isinstance(destination, bytearray):
            destination.extend(self.value)
        elif isinstance(destination, MemDataItem):
            destination.copy_data_from(self)
        elif isinstance(destination, DataItemABC):
            destination.copy_data_from(self.value)
        else:
            # Assume file like object
            # noinspection PyUnresolvedReferences
            destination.write(self.value)

    def copy_data_from(self, source):
        if isinstance(source, str) or isinstance(source, Path):
            with open(source, 'rb') as reader:
                self._copy_data_from_reader(reader)
        if isinstance(source, bytes) or isinstance(source, bytearray):
            if len(source) > self.max_size:
                raise PersistyError('data_overflow')
            self._update_value(bytes(source))
        elif isinstance(source, MemDataItem):
            if len(source.value) > self.max_size:
                raise PersistyError('data_overflow')
            self.value = bytes(source.value)
            self.updated_at = datetime.now()
            self._etag = source._etag
        elif isinstance(source, DataItemABC):
            if source.size > self.max_size:
                raise PersistyError('data_overflow')
            writer = io.BytesIO()
            source.copy_data_to(writer)
            self._update_value(writer.getvalue())
        else:
            # noinspection PyUnresolvedReferences
            self._copy_data_from_reader(source)

    def _copy_data_from_reader(self, source):
        value = source.read(self.max_size)
        if len(value) == self.max_size:
            # noinspection PyUnresolvedReferences
            overflow = source.read(1)
            if overflow:
                raise PersistyError('data_overflow')
        self._update_value(value)

    def _update_value(self, value: bytes):
        self.value = value
        self.updated_at = datetime.now()
        self._etag = UNDEFINED
