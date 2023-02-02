from dataclasses import dataclass
from typing import Optional, Iterator

import boto3

from servey.security.authorization import Authorization

from persisty.secured.secured_store_factory_abc import SecuredStoreFactoryABC
from persisty_data.content_meta import ContentMeta
from persisty_data.data_store_abc import DataStoreABC, _ROUTE
from persisty_data.form_field import FormField
from persisty_data.upload_config import UploadConfig

_S3_CLIENT = boto3.client('s3')


@dataclass
class S3DataStore(DataStoreABC):
    name: str
    bucket_name: str
    content_meta_store_factory: SecuredStoreFactoryABC[ContentMeta]
    upload_expire_in: int = 3600
    max_file_size: int = 100 * 1024 * 1024
    download_expire_in: int = 3600

    def get_name(self) -> str:
        return self.name

    def create_routes(self) -> Iterator[_ROUTE]:
        # No routes required as S3 handles it all
        pass

    def url_for_download(self, authorization: Optional[Authorization], key: str) -> Optional[str]:
        response = _S3_CLIENT.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': self.bucket_name,
                'Key': key
            },
            ExpiresIn=self.download_expire_in
        )
        return response

    def config_for_upload(self, authorization: Optional[Authorization], key: Optional[str]) -> UploadConfig:
        response = _S3_CLIENT.generate_presigned_post(
            Bucket=self.bucket_name,
            Key=key,
            Conditions=[["content-length-range", 0, self.max_file_size]],
            ExpiresIn=3600
        )
        fields = response.get('fields') or {}
        pre_populated_fields = [
            FormField(k, v) for k, v in fields.items()
        ]
        return UploadConfig(
            url=response['url'],
            pre_populated_fields=pre_populated_fields
        )
