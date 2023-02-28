from dataclasses import dataclass
from typing import Optional, Iterator

import boto3
from servey.security.authorization import Authorization

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty_data.data_store_abc import DataStoreABC, _CHUNK, _UPLOAD, _CONTENT_META, _ROUTE
from persisty_data.form_field import FormField
from persisty_data.upload_form import UploadForm

_S3_CLIENT = boto3.client('s3')


@dataclass
class S3DataStore(DataStoreABC):
    name: str
    bucket_name: str
    upload_expire_in: int = 3600
    max_file_size: int = 100 * 1024 * 1024
    download_expire_in: int = 3600
    public_download_path: Optional[str] = None

    def get_name(self) -> str:
        return self.name

    def create_routes(self) -> Iterator[_ROUTE]:
        pass  # No need - routes are in S3

    def get_content_meta_store_factory(self) -> StoreFactoryABC[_CONTENT_META]:
        pass

    def get_upload_store_factory(self) -> StoreFactoryABC[_UPLOAD]:
        pass

    def get_chunk_store_factory(self) -> StoreFactoryABC[_CHUNK]:
        pass

    def url_for_download(self, authorization: Optional[Authorization], key: str) -> Optional[str]:
        if self.public_download_path:
            result = self.public_download_path.format(**{"key": key})
            return result
        response = _S3_CLIENT.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': self.bucket_name,
                'Key': key
            },
            ExpiresIn=self.download_expire_in
        )
        return response

    def form_for_upload(self, authorization: Authorization, key: Optional[str]) -> UploadForm:
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
        return UploadForm(
            url=response['url'],
            pre_populated_fields=pre_populated_fields
        )