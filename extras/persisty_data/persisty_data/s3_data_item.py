import io
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict

# noinspection PyPackageRequirements
from botocore.exceptions import ClientError

from persisty.util import UNDEFINED
from persisty_data.data_item_abc import DataItemABC
from persisty_data.s3_client import get_s3_client


@dataclass
class S3DataItem(DataItemABC):
    bucket_name: str
    key: str
    download_expire_in: Optional[int] = 3600

    def __post_init__(self):
        self.reset_meta()

    def load_meta(self):
        try:
            response = get_s3_client().head_object(
                Bucket=self.bucket_name, Key=self.key
            )
            self._init_meta_from_response(response)
        except ClientError:
            self._init_meta_from_response({})

    # noinspection PyAttributeOutsideInit
    def reset_meta(self):
        self._updated_at = UNDEFINED
        self._etag = UNDEFINED
        self._content_type = UNDEFINED
        self._size = UNDEFINED
        self._data_url = UNDEFINED

    def _init_meta_from_response(self, response: Dict):
        self._updated_at = response.get("LastModified")
        self._etag = response.get("ETag")
        self._content_type = response.get("ContentType")
        self._size = response.get("ContentLength")

    @property
    def updated_at(self) -> Optional[datetime]:
        updated_at = getattr(self, "_updated_at", UNDEFINED)
        if updated_at is not UNDEFINED:
            return updated_at
        self.load_meta()
        return self._updated_at

    @property
    def etag(self) -> Optional[str]:
        etag = getattr(self, "_etag", UNDEFINED)
        if etag is not UNDEFINED:
            return etag
        self.load_meta()
        return self._etag

    @property
    def content_type(self) -> Optional[str]:
        content_type = getattr(self, "_content_type", UNDEFINED)
        if content_type is not UNDEFINED:
            return content_type
        self.load_meta()
        return self._content_type

    @property
    def size(self) -> Optional[int]:
        size = getattr(self, "_size", UNDEFINED)
        if size is not UNDEFINED:
            return size
        self.load_meta()
        return self._size

    @property
    def data_url(self) -> Optional[str]:
        data_url = getattr(self, "_data_url", UNDEFINED)
        if data_url is not UNDEFINED:
            return data_url
        response = get_s3_client().generate_presigned_url(
            "get_object",
            Params={"Bucket": self.bucket_name, "Key": self.key},
            ExpiresIn=self.download_expire_in,
        )
        return response

    def get_data_reader(self) -> io.IOBase:
        response = get_s3_client().get_object(Bucket=self.bucket_name, Key=self.key)
        self._init_meta_from_response(response)
        return response["Body"]


@dataclass
class _S3Writer(io.RawIOBase):
    s3_data_item: S3DataItem
