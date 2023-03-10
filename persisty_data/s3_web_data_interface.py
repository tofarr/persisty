from dataclasses import dataclass
from typing import Optional

from servey.security.authorization import Authorization

from persisty_data.form_field import FormField
from persisty_data.s3_client import get_s3_client
from persisty_data.upload_form import UploadForm
from persisty_data.web_data_interface_abc import WebDataInterfaceABC


@dataclass
class S3WebDataInterface(WebDataInterfaceABC):
    bucket_name: str
    max_size: int
    download_expire_in: int
    public_download_path: Optional[str] = None

    def get_upload_form(self, key: str, authorization: Optional[Authorization]) -> UploadForm:
        response = get_s3_client().generate_presigned_post(
            Bucket=self.bucket_name,
            Key=key,
            Conditions=[["content-length-range", 0, self.max_size]],
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

    def get_download_url(self, key: str, authorization: Optional[Authorization]) -> str:
        if self.public_download_path:
            result = self.public_download_path.format(**{"key": key})
            return result
        response = get_s3_client().generate_presigned_url(
            'get_object',
            Params={
                'Bucket': self.bucket_name,
                'Key': key
            },
            ExpiresIn=self.download_expire_in
        )
        return response
