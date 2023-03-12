import dataclasses
from datetime import datetime
from typing import Optional, Iterator

import marshy
from dateutil.relativedelta import relativedelta
from servey.security.authorization import Authorization
from servey.security.authorizer.authorizer_abc import AuthorizerABC
from servey.security.authorizer.authorizer_factory_abc import get_default_authorizer

from persisty.errors import PersistyError
from persisty.factory.store_factory_abc import ROUTE
from persisty.store_meta import StoreMeta
from persisty.util import UNDEFINED
from persisty_data.data_store_abc import DataStoreABC
from persisty_data.data_store_factory_abc import DataStoreFactoryABC
from persisty_data.form_field import FormField
from persisty_data.hosted_data_store import HostedDataStore
from persisty_data.upload_form import UploadForm


@dataclasses.dataclass
class HostedDataStoreFactory(DataStoreFactoryABC):
    """
    Services like s3 allow uploading and downloading files through pre-signed urls. This emulates that functionality
    in a hosted environment using Starlette Routes
    """
    data_store_factory: DataStoreFactoryABC
    authorizer: Optional[AuthorizerABC] = None
    " Can we derive the download and upload paths from actions? It feels like they should be connected."
    secured_upload_path: Optional[str] = None
    upload_expire_in: int = 3600
    secured_download_path: Optional[str] = None
    download_expire_in: int = 3600
    public_download_path: Optional[str] = None
    _meta: StoreMeta = UNDEFINED

    def get_meta(self) -> StoreMeta:
        return self.data_store_factory.get_meta()

    def create(self, authorization: Optional[Authorization]) -> Optional[DataStoreABC]:
        result = HostedDataStore(
            data_store=self.data_store_factory.create(authorization),
            get_download_url=self.get_download_url,
            authorization=authorization
        )
        return result

    def get_upload_form(self, key: str, authorization: Optional[Authorization]) -> UploadForm:
        if not self.secured_upload_path:
            raise PersistyError('unavailable_operation')
        data_store = self.data_store_factory.create(authorization)
        item = data_store.read(key)
        store_access = data_store.get_meta().store_access
        if item:
            if not store_access.updatable:
                raise PersistyError('unavailable_operation')
        else:
            if not store_access.creatable:
                raise PersistyError('unavailable_operation')
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
        if self.public_download_path:
            result = self.public_download_path.format(**{"key": key})
            return result
        if not self.secured_download_path:
            raise PersistyError('unavailable_operation')
        expire_at = datetime.now() + relativedelta(seconds=self.download_expire_in)
        download_authorization = Authorization(
            authorization.subject_id, [f'download:{key}'], datetime.now(), expire_at
        )
        token = self.authorizer.encode(download_authorization)
        result = self.secured_download_path.format(token=token)
        return result

    def create_routes(self) -> Iterator[ROUTE]:
        """
        Create routes for this factory. In hosted mode, uploads and downloads may go through python.
        In a lambda environment, uploads and downloads should be based on S3 and not go through the python
        environment.
        """
        routes = [
            self.create_route_for_public_download(),
            self.create_route_for_secured_download(),
            self.create_route_for_secured_upload(),
        ]
        yield from (r for r in routes if r)

    def create_route_for_public_download(self) -> Optional[ROUTE]:
        if not self.public_download_path:
            return
        from starlette.routing import Route
        from starlette.requests import Request
        from starlette.responses import Response
        from persisty_data.data_item_response import data_item_response
        store_meta = self.get_meta()

        def download(request: Request) -> Response:
            key = request.path_params.get('key')
            data_store = self.data_store_factory.create(None)
            data_item = data_store.read(key)
            if not data_item:
                return Response(status_code=404)

            return data_item_response(
                request_headers=request.headers,
                data_item=data_item,
                cache_control=store_meta.cache_control
            )

        path = self.public_download_path.replace('{key}', '{key:path}')
        return Route(
            path, name=store_meta.name+'_public_download', endpoint=download, methods=('GET',)
        )

    def create_route_for_secured_download(self) -> Optional[ROUTE]:
        if not self.secured_download_path:
            return
        from starlette.routing import Route
        from starlette.requests import Request
        from starlette.responses import Response
        from persisty_data.data_item_response import data_item_response
        store_meta = self.get_meta()

        def download(request: Request) -> Response:
            token = request.path_params.get('token')
            authorization = self.authorizer.authorize(token)
            key = next(s.split('upload:')[-1] for s in authorization.scopes if s.startswith('upload:'))
            data_store = self.data_store_factory.create(authorization)
            data_item = data_store.read(key)
            return data_item_response(
                request_headers=request.headers,
                data_item=data_item,
                cache_control=store_meta.cache_control
            )

        return Route(
            self.secured_download_path, name=store_meta.name + '_secured_download', endpoint=download, methods=('GET',)
        )

    def create_route_for_secured_upload(self) -> Optional[ROUTE]:
        from starlette.routing import Route
        from starlette.requests import Request
        from starlette.responses import JSONResponse, Response
        from starlette.datastructures import UploadFile
        store_meta = self.get_meta()

        async def upload(request: Request) -> Response:
            form = await request.form()
            token = form.get('token')
            authorization = self.authorizer.authorize(token)
            key = next(s.split('upload:')[-1] for s in authorization.scopes if s.startswith('upload:'))
            data_store = self.create(authorization)
            form_file: UploadFile = form['file']
            with data_store.get_data_writer(key, form_file.content_type) as writer:
                while True:
                    buffer = await form_file.read(1024 * 64)
                    if buffer:
                        writer.write(buffer)
                    else:
                        break
            item = data_store.read(key)
            return JSONResponse(status_code=200, content=marshy.dump(item))

        return Route(
            self.secured_upload_path,
            name=store_meta.name + '_upload',
            endpoint=upload,
            methods=('POST', 'PUT', 'PATCH')
        )


def hosted_data_store_factory(
    data_store_factory: DataStoreFactoryABC,
    authorizer: Optional[AuthorizerABC] = None,
    upload_expire_in: int = 3600,
    download_expire_in: int = 3600,
):
    if not authorizer:
        authorizer = get_default_authorizer()
    name = data_store_factory.get_meta().name.replace('_', '-')
    factory = HostedDataStoreFactory(
        data_store_factory=data_store_factory,
        authorizer=authorizer,
        secured_upload_path=f"/data/{name}/upload",
        upload_expire_in=upload_expire_in,
        secured_download_path="/data/" + name + "/secure/{token}",
        download_expire_in=download_expire_in,
        public_download_path="/data/" + name + "/public/{key}",
    )
    return factory
