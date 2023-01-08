from dataclasses import dataclass
from typing import Optional, IO
from uuid import uuid4

import boto3
from servey.cache_control.cache_control_abc import CacheControlABC

from persisty.access_control.access_control_abc import AccessControlABC
from persisty.data_storage.data_item import DataItemFilter, DataItem
from persisty.data_storage.data_storage_abc import DataStorageABC
from persisty.storage.result_set import ResultSet

S3_CLIENT = boto3.client('s3')

I THINK WE SHOULD FOLLOW THE S3 API CLOSER - WITH RANGES!


@dataclass
class S3DataStorage(DataStorageABC):
    name: str
    bucket_name: str
    access_control: AccessControlABC
    cache_control: CacheControlABC
    public_url_pattern = Optional[str]

    def get_name(self) -> str:
        return self.name

    def get_access_control(self) -> AccessControlABC:
        return self.access_control

    def get_cache_control(self) -> CacheControlABC:
        return self.cache_control

    def get_signed_url_for_put(self, key: Optional[str] = None, exp: int = 3600) -> str:
        if not key:
            key = str(uuid4())
        response = S3_CLIENT.generate_presigned_post(
            Bucket=self.bucket_name,
            Key=key,
            ExpiresIn=exp,
        )
        url = response['url']
        return url

    def put(self, data: IO, key: Optional[str] = None, mime_type: Optional[str] = None) -> DataItem:
        if not key:
            key = str(uuid4())
        kwargs = dict(
            Bucket=self.bucket_name,
            Key=key,
            Body=data
        )
        if mime_type:
            kwargs['ContentType'] = mime_type
        S3_CLIENT.put_object(**kwargs)
        return self.read(key)

    def get_url_for_read(self, key: str, exp: Optional[int] = None) -> str:
        if not exp:
            return self.public_url_pattern.format(key=key)
        url = S3_CLIENT.generate_presigned_url(
            ClientMethod='get_object',
            Params=dict(
                Bucket=self.bucket_name,
                Key=key,
            ),
            ExpiresIn=exp
        )
        return url

    def open_for_read(self, key: str) -> Optional[IO]:
        THIS IS WRONG. IT MAY BE EASIER TO DOWNLOAD OR MAKE THIS ASYNC
        IT IS FOCUSED AROUND SMALL FILES
        MAYBE READING TO A BUFFER SHOULD BE SUPPORTED?
        ByteBuffer

    def read(self, key: str) -> Optional[DataItem]:
        response = S3_CLIENT.get_obejct(
            Bucket=self.bucket_name,
            Key=key
        )
        return DataItem(
            key=key,
            size_in_bytes=response['ContentLength'],
            updated_at=response['LastModified'],
            etag=response['ETag'],
            mime_type=response.get('ContentType')
        )

    def delete(self, key: str) -> bool:
        response = S3_CLIENT.delete_object(
            Bucket=self.bucket_name,
            Key=key
        )
        return response['DeleteMarker']

    def search(self, search_filter: Optional[DataItemFilter] = None, page_key: Optional[str] = None,
               limit: int = 100) -> ResultSet[DataItem]:
        aaa()
