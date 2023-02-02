import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple

from dateutil.relativedelta import relativedelta
from servey.action.action import action, get_action
from servey.security.authorization import Authorization
from servey.security.authorizer.authorizer_abc import AuthorizerABC
from servey.security.authorizer.authorizer_factory_abc import get_default_authorizer
from servey.trigger.fixed_rate_trigger import FixedRateTrigger

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.errors import PersistyError
from persisty.factory.store_factory_abc import StoreFactoryABC

from persisty.store.store_abc import StoreABC
from persisty_data.chunk import Chunk
from persisty_data.chunk_store import ChunkStore
from persisty_data.content_meta import ContentMeta
from persisty_data.content_meta_store import ContentMetaStore

from persisty_data.data_store_abc import DataStoreABC, _ROUTE
from persisty_data.upload import Upload, UploadStatus
from persisty_data.upload_config import UploadConfig
from persisty_data.upload_store import UploadStore

LOGGER = logging.getLogger(__name__)


@dataclass
class DataStore(DataStoreABC):
    name: str
    content_meta_store_factory: StoreFactoryABC[ContentMeta]
    upload_store_factory: StoreFactoryABC[Upload]
    chunk_store_factory: StoreFactoryABC[Chunk]
    upload_store: StoreABC[Upload]
    authorizer: AuthorizerABC
    secured_download_path: Optional[str] = None
    secured_upload_path: Optional[str] = None
    public_download_path: Optional[str] = None
    download_expire_in: int = 3600
    upload_expire_in: int = 3600
    max_chunk_size: int = 5 * 1024 * 1024
    max_file_size: int = 100 * 1024 * 1024

    def __post_init__(self):
        if self.authorizer == get_default_authorizer():
            # We required that you don't use the same authorizer for everything - otherwise somebody getting one
            # of these urls could effectively allow them to impersonate somebody - especially if developers
            # check only the subject_id in the Authorization object
            raise PersistyError('distinct_authorizer_required')

    def get_name(self):
        return self.name

    def create_routes(self):
        routes = [
            self.create_route_for_public_download(),
            self.create_route_for_secured_download(),
            self.create_route_for_secured_upload(),
        ]
        yield from (r for r in routes if r)

    def create_actions(self):
        yield from super().create_actions()
        yield self._create_clear_stale_upload_action()

    def _create_clear_stale_upload_action(self):
        action_name = self.name + '_clear_stale_uploads'

        @action(
            name=action_name,
            triggers=FixedRateTrigger(self.upload_expire_in)
        )
        def clear_stale_uploads():

            created__lte = datetime.now() - relativedelta(seconds=self.upload_expire_in)
            search_filter = (
                AttrFilter('created_at', AttrFilterOp.lte, created__lte) &
                AttrFilter('status', AttrFilterOp.eq, UploadStatus.IN_PROGRESS)
            )
            uploads = self.upload_store.search_all(search_filter)
            edits = (
                BatchEdit(update_item=Upload(id=upload.id, status=UploadStatus.TIMED_OUT))
                for upload in uploads
            )
            self.upload_store.edit_all(edits)

        return get_action(clear_stale_uploads)

    def url_for_download(self, authorization: Optional[Authorization], key: str) -> Optional[str]:
        content_meta_store = self.content_meta_store_factory.create(authorization)
        chunk_store = self.chunk_store_factory.create(authorization)
        if (
            not content_meta_store.get_meta().store_access.readable
            or not chunk_store.get_meta().store_access.readable
        ):
            raise PersistyError('unavailable_operation')
        content_meta = content_meta_store.read(key)
        if content_meta is None:
            return
        if self.public_download_path:
            result = self.public_download_path.format(key=key)
            return result
        if not self.secured_download_path:
            raise PersistyError('unavailable_operation')
        expire_at = datetime.now() + relativedelta(seconds=self.download_expire_in)
        upload_authorization = Authorization(
            authorization.subject_id, [f'download:{key}'], datetime.now(), expire_at
        )
        token = self.authorizer.encode(upload_authorization)
        result = self.secured_download_path.format(token=token)
        return result

    def config_for_upload(
        self,
        authorization: Optional[Authorization],
        key: Optional[str]
    ) -> UploadConfig:
        if not self.secured_download_path and not self.secured_download_path:
            raise PersistyError('unavailable_operation')
        content_meta_store = self.content_meta_store_factory.create(authorization)
        chunk_store = self.chunk_store_factory.create(authorization)
        content_access = content_meta_store.get_meta().store_access
        chunk_access = chunk_store.get_meta().store_access
        content_meta = content_meta_store.read(key) if key else None
        if content_meta:
            if not content_access.updatable or not chunk_access.updatable:
                raise PersistyError('not_permitted')
        else:
            if not content_access.creatable or not chunk_access.creatable:
                raise PersistyError('not_permitted')

        expire_at = datetime.now() + relativedelta(seconds=self.download_expire_in)
        upload_authorization = Authorization(
            authorization.subject_id, [f'upload:{key}'], datetime.now(), expire_at
        )
        token = self.authorizer.encode(upload_authorization)
        url = self.secured_upload_path.format(token=token)
        return UploadConfig(url=url)

    def create_route_for_public_download(self) -> Optional[_ROUTE]:
        if not self.public_download_path:
            return
        from starlette.routing import Route
        from starlette.requests import Request
        from starlette.responses import Response
        from persisty_data.chunk_response import chunk_response

        def download(request: Request) -> Response:
            key = request.path_params.get('key')
            return chunk_response(
                key,
                None,
                request.headers,
                self.content_meta_store_factory,
                self.chunk_store_factory
            )

        return Route(
            self.public_download_path, name=self.name+'_public_download', endpoint=download, methods=('GET',)
        )

    def create_route_for_secured_download(self) -> Optional[_ROUTE]:
        if not self.secured_download_path:
            return
        from starlette.routing import Route
        from starlette.requests import Request
        from starlette.responses import Response
        from persisty_data.chunk_response import chunk_response

        def download(request: Request) -> Response:
            token = request.path_params.get('token')
            authorization = self.authorizer.authorize(token)
            key = next(s.split('upload:') for s in authorization.scopes if s.startswith('upload:'))
            return chunk_response(
                key,
                authorization,
                request.headers,
                self.content_meta_store_factory,
                self.chunk_store_factory
            )

        return Route(
            self.secured_download_path, name=self.name + '_secured_download', endpoint=download, methods=('GET',)
        )

    def create_route_for_secured_upload(self) -> Optional[_ROUTE]:
        from starlette.routing import Route
        from starlette.requests import Request
        from starlette.responses import Response
        from starlette.datastructures import UploadFile

        async def upload(request: Request) -> Response:
            token = request.path_params.get('token')
            authorization = self.authorizer.authorize(token)
            key = next(s.split('download:') for s in authorization.scopes if s.startswith('download:'))

            form = await request.form()
            form_file: UploadFile = form['file']
            assert len(form_file) <= self.max_file_size

            upload_store = self.upload_store_factory.create(None)
            upload_ = upload_store.create(Upload(
                content_key=key,
                content_type=form_file.content_type
            ))

            chunk_store = self.chunk_store_factory.create(None)
            part_number = 1
            while True:
                data = await form_file.read(self.max_chunk_size)
                if not data:
                    upload_.status = UploadStatus.COMPLETED
                    upload_store.update(upload_)
                    return Response(200, b'')
                chunk_store.create(Chunk(
                    content_key=key,
                    part_number=part_number,
                    upload_id=upload_.id,
                    data=data
                ))

        return Route(
            self.secured_upload_path, name=self.name + '_upload', endpoint=upload, methods=('POST', 'PUT', 'PATCH')
        )


def wrap_stores(
    content_meta_store: StoreABC[ContentMeta],
    chunk_store: StoreABC[Chunk],
    upload_store: StoreABC[Upload]
) -> Tuple[
     StoreABC[ContentMeta],
     StoreABC[Chunk],
     StoreABC[Upload]
]:
    return [
        ContentMetaStore(content_meta_store, chunk_store),
        ChunkStore(chunk_store, upload_store),
        UploadStore(upload_store, chunk_store, content_meta_store)
    ]
