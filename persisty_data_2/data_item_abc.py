import hashlib
import io
from abc import abstractmethod, ABC
from datetime import datetime
from typing import Optional

from schemey.schema import str_schema, int_schema

from persisty.attr.attr import Attr
from persisty.attr.attr_filter_op import STRING_FILTER_OPS, SORTABLE_FILTER_OPS, FILTER_OPS
from persisty.attr.attr_type import AttrType
from persisty.key_config.attr_key_config import AttrKeyConfig
from persisty.store_meta import StoreMeta


class DataItemABC(ABC):

    @property
    @abstractmethod
    def key(self) -> str:
        """
        Get the key for this item
        """

    @property
    @abstractmethod
    def updated_at(self) -> Optional[datetime]:
        """
        Get the last modified date for this item. Returns none if the item was never updated
        """

    @property
    @abstractmethod
    def etag(self) -> Optional[str]:
        """
        Get the etag for this resource. Return none if the item has no content
        """

    @property
    @abstractmethod
    def content_type(self) -> Optional[str]:
        """
        Get the mime type for this item
        """

    @property
    @abstractmethod
    def size(self) -> Optional[int]:
        """
        Get the size in bytes of this item
        """

    @property
    @abstractmethod
    def max_size(self) -> int:
        """
        Get the max size for this data item
        """

    @abstractmethod
    def get_data_reader(self) -> io.IOBase:
        """
        Get a reader for this item
        """

    @abstractmethod
    def copy_data_to(self, destination):
        """
        Copy this resource to a bytearray, file like object, a string or path representing a file, or a DataItemABC.
        Content Types are unaffected by this operation
        """

    @abstractmethod
    def copy_data_from(self, source):
        """
        Upload this resource from a bytes, bytearray, file like object, a string or path representing a file or
        DataItemABC. Content Types are unaffected by this operation
        """


def calculate_etag(file, buffer_size: int) -> str:
    md5 = hashlib.md5()
    while True:
        bytes_ = file.read(buffer_size)
        if not bytes_:
            break
        md5.update(bytes_)
    result = md5.hexdigest()
    return result


DATA_ITEM_META = StoreMeta(
    name='data_item',
    attrs=(
        Attr('key', AttrType.STR, str_schema(max_length=255), sortable=True, permitted_filter_ops=STRING_FILTER_OPS),
        Attr('size', AttrType.INT, int_schema(), sortable=True, permitted_filter_ops=SORTABLE_FILTER_OPS),
        Attr('content_type', AttrType.STR, str_schema(max_length=255), sortable=True,
             permitted_filter_ops=STRING_FILTER_OPS),
        Attr('etag', AttrType.STR, str_schema(max_length=255), sortable=False, permitted_filter_ops=FILTER_OPS),
    ),
    key_config=AttrKeyConfig('key')
)
