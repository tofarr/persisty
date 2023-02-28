import io
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict

from botocore.exceptions import ClientError

from persisty.util import UNDEFINED
from persisty_data_2.data_item_abc import DataItemABC
from persisty_data_2.file_data_item import FileDataItem
from persisty_data_2.mem_data_item import MemDataItem
from persisty_data_2.s3_client import get_s3_client


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

    def copy_data_to(self, destination):
        if isinstance(destination, str) or isinstance(destination, Path):
            Path(destination).parent.mkdir(parents=True, exist_ok=True)
            with open(destination, 'wb') as writer:
                get_s3_client().download_fileobj(Bucket=self.bucket_name, Key=self.key, Fileobj=writer)
        elif isinstance(destination, bytearray):
            writer = io.BytesIO()
            get_s3_client().download_fileobj(Bucket=self.bucket_name, Key=self.key, Fileobj=writer)
            destination.extend(writer.getvalue())
        elif isinstance(destination, DataItemABC):
            response = get_s3_client().get_object(Bucket=self.bucket_name, Key=self.key)
            destination.copy_data_from(response['Body'])
        else:
            get_s3_client().download_fileobj(Bucket=self.bucket_name, Key=self.key, Fileobj=destination)

    def copy_data_from(self, source):
        if isinstance(source, str) or isinstance(source, Path):
            with open(source, 'rb') as reader:
                get_s3_client().upload_fileobj(Bucket=self.bucket_name, Key=self.key, Fileobj=reader)
        elif isinstance(source, bytes) or isinstance(source, bytearray):
            get_s3_client().upload_fileobj(Bucket=self.bucket_name, Key=self.key, Fileobj=io.BytesIO(source))
        elif isinstance(source, MemDataItem):
            get_s3_client().upload_fileobj(Bucket=self.bucket_name, Key=self.key, Fileobj=io.BytesIO(source.value))
        elif isinstance(source, FileDataItem):
            with open(source.path, 'rb') as writer:
                get_s3_client().upload_fileobj(Bucket=self.bucket_name, Key=self.key, Fileobj=writer)
        elif isinstance(source, DataItemABC):
            with source.get_data_reader() as reader:
                get_s3_client().upload_fileobj(Bucket=self.bucket_name, Key=self.key, Fileobj=reader)
        else:
            get_s3_client().upload_fileobj(Bucket=self.bucket_name, Key=self.key, Fileobj=source)


@dataclass
class _S3Writer(io.RawIOBase):
    s3_data_item: S3DataItem
