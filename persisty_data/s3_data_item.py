import io
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict

from botocore.exceptions import ClientError

from persisty.util import UNDEFINED
from persisty_data.data_item_abc import DataItemABC
from persisty_data.s3_client import get_s3_client


@dataclass
class S3DataItem(DataItemABC):
    bucket_name: str
    key: str
    max_size: int = 1024 * 1024 * 100  # Default 100mb - seems fair
    _updated_at: Optional[datetime] = UNDEFINED
    _etag: Optional[str] = UNDEFINED
    _content_type: Optional[str] = UNDEFINED

    def load_meta(self):
        try:
            response = get_s3_client().head_object(
                Bucket=self.bucket_name,
                Key=self.key
            )
            self._init_meta_from_response(response)
        except ClientError:
            self._init_meta_from_response({})

    def _init_meta_from_response(self, response: Dict):
        self._updated_at = response.get('LastModified')
        self._etag = response.get('ETag')
        self._content_type = response.get('ContentType')

    @property
    def updated_at(self) -> Optional[datetime]:
        updated_at = self._updated_at
        if updated_at is not UNDEFINED:
            return updated_at
        self.load_meta()
        return self._updated_at

    @property
    def etag(self) -> Optional[str]:
        etag = self._etag
        if etag is not UNDEFINED:
            return etag
        self.load_meta()
        return self._etag

    @property
    def content_type(self) -> Optional[str]:
        content_type = self._content_type
        if content_type is not UNDEFINED:
            return content_type
        self.load_meta()
        return self._content_type

    @property
    def size(self) -> Optional[int]:
        etag = self._etag
        if etag is not UNDEFINED:
            return etag
        self.load_meta()
        return self._etag

    def get_data_reader(self) -> io.IOBase:
        response = get_s3_client().get_object(Bucket=self.bucket_name, Key=self.key)
        self._init_meta_from_response(response)
        return response['Body']


@dataclass
class _S3Writer(io.RawIOBase):
    s3_data_item: S3DataItem
