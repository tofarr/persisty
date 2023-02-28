from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from dateutil.relativedelta import relativedelta
from servey.security.authorization import Authorization, AuthorizationError
from servey.security.authorizer.authorizer_abc import AuthorizerABC

from persisty.errors import PersistyError
from persisty_data_2.form_field import FormField
from persisty_data_2.upload_form import UploadForm
from persisty_data_2.web_data_interface_abc import WebDataInterfaceABC


@dataclass
class WebDataInterface(WebDataInterfaceABC):
    authorizer: AuthorizerABC
    secured_upload_path: Optional[str] = None
    upload_expire_in: int = 3600


    def get_upload_form(self, key: str, authorization: Optional[Authorization]) -> UploadForm:
        if not self.secured_upload_path:
            raise PersistyError('unavailable_operation')
        if not authorization:
            raise AuthorizationError()
        content_meta_store = self.content_meta_store_factory.create(authorization)
        chunk_store = self.chunk_store_factory.create(authorization)
        content_meta_access = content_meta_store.get_meta().store_access
        chunk_access = chunk_store.get_meta().store_access
        content_meta = content_meta_store.read(key) if key else None
        if content_meta:
            if not content_meta_access.updatable or not chunk_access.updatable:
                raise AuthorizationError()
        else:
            if not content_meta_access.creatable or not chunk_access.creatable:
                raise AuthorizationError()

        expire_at = datetime.now() + relativedelta(seconds=self.upload_expire_in)
        upload_authorization = Authorization(
            authorization.subject_id, [f'upload:{key}'], datetime.now(), expire_at
        )
        token = self.authorizer.encode(upload_authorization)
        return UploadForm(
            url=self.secured_upload_path,
            pre_populated_fields=[
                FormField('token', token)
            ]
        )

    def get_download_url(self, key: str, authorization: Optional[Authorization]) -> str:
        pass