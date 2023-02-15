import dataclasses
import itertools
import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Tuple, Iterator

import marshy
from dateutil.relativedelta import relativedelta
from servey.action.action import action, get_action
from servey.security.authorization import Authorization, AuthorizationError
from servey.security.authorizer.authorizer_abc import AuthorizerABC
from servey.security.authorizer.authorizer_factory_abc import get_default_authorizer
from servey.trigger.fixed_rate_trigger import FixedRateTrigger
from starlette.responses import JSONResponse

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.batch_edit import BatchEdit
from persisty.errors import PersistyError
from persisty.factory.default_store_factory import DefaultStoreFactory
from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.impl.default_store import DefaultStore

from persisty.store.store_abc import StoreABC
from persisty.store_meta import get_meta
from persisty.stored import stored
from persisty_data.chunk import Chunk
from persisty_data.chunk_store import ChunkStore
from persisty_data.content_meta import ContentMeta
from persisty_data.content_meta_store import ContentMetaStore

from persisty_data.data_store_abc import DataStoreABC, _ROUTE
from persisty_data.form_field import FormField
from persisty_data.owned_upload_store_factory import OwnedUploadStoreFactory
from persisty_data.upload import Upload, UploadStatus
from persisty_data.upload_form import UploadForm
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

    def get_content_meta_store_factory(self) -> StoreFactoryABC[ContentMeta]:
        return self.content_meta_store_factory

    def get_upload_store_factory(self) -> StoreFactoryABC[Upload]:
        return self.upload_store_factory

    def get_chunk_store_factory(self) -> StoreFactoryABC[Chunk]:
        return self.chunk_store_factory

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
        if not chunk_store.get_meta().store_access.readable:
            raise PersistyError('unavailable_operation')
        content_meta = content_meta_store.read(key)
        return self._get_url_for_content_meta(authorization, key, content_meta)

    def _get_url_for_content_meta(self, authorization: Optional[Authorization], key: str, content_meta: ContentMeta):
        if content_meta is None:
            return
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

    def all_urls_for_download(
        self,
        authorization: Optional[Authorization],
        keys: Iterator[str]
    ) -> Iterator[Optional[str]]:
        content_meta_store = self.content_meta_store_factory.create(authorization)
        chunk_store = self.chunk_store_factory.create(authorization)
        if not chunk_store.get_meta().store_access.readable:
            raise PersistyError('unavailable_operation')
        while True:
            key_batch = list(itertools.islice(keys, content_meta_store.get_meta().batch_size))
            if not key_batch:
                return
            content_meta_batch = content_meta_store.read_batch(key_batch)
            for key, content_meta in zip(key_batch, content_meta_batch):
                yield self._get_url_for_content_meta(authorization, key, content_meta)

    def form_for_upload(
        self,
        authorization: Authorization,
        key: Optional[str]
    ) -> UploadForm:
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

        expire_at = datetime.now() + relativedelta(seconds=self.download_expire_in)
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

        path = self.public_download_path.replace('{key}', '{key:path}')
        return Route(
            path, name=self.name+'_public_download', endpoint=download, methods=('GET',)
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
            key = next(s.split('upload:')[-1] for s in authorization.scopes if s.startswith('upload:'))
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
            assert int(request.headers['content-length']) <= self.max_file_size
            form = await request.form()
            token = form.get('token')
            authorization = self.authorizer.authorize(token)
            key = next(s.split('upload:')[-1] for s in authorization.scopes if s.startswith('upload:'))
            form_file: UploadFile = form['file']
            upload_store = self.upload_store_factory.create(authorization)
            upload_ = upload_store.create(Upload(
                content_key=key,
                content_type=form_file.content_type
            ))
            chunk_store = self.chunk_store_factory.create(authorization)
            part_number = 1
            while True:
                data = await form_file.read(self.max_chunk_size)
                if not data:
                    upload_.status = UploadStatus.COMPLETED
                    upload_store.update(upload_)
                    return JSONResponse(status_code=200, content=marshy.dump(upload_))
                chunk_store.create(Chunk(
                    content_key=upload_.content_key,
                    part_number=part_number,
                    upload_id=upload_.id,
                    data=data
                ))

        return Route(
            self.secured_upload_path, name=self.name + '_upload', endpoint=upload, methods=('POST', 'PUT', 'PATCH')
        )


def bind_stores(
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


def create_default_stores_for_data(name: str) -> Tuple[
    StoreABC[ContentMeta],
    StoreABC[Chunk],
    StoreABC[Upload]
]:
    """
    Given a name to be used for a data store, create default stores
    """
    class_name = name.title().replace('_', '')
    content_meta_params = {'__annotations__': {}}
    content_meta = stored(type(f"{class_name}ContentMeta", (ContentMeta,), content_meta_params))
    chunk = stored(type(f"{class_name}Chunk", (Chunk,), {'__annotations__': {}}))
    upload = stored(type(f"{class_name}Upload", (Upload,), {'__annotations__': {}}))
    return bind_stores(
        DefaultStore(get_meta(content_meta)),
        DefaultStore(get_meta(chunk)),
        DefaultStore(get_meta(upload))
    )


def secured_upload_public_download_data_store(
    content_meta_store: StoreABC[ContentMeta],
    chunk_store: StoreABC[ContentMeta],
    upload_store: StoreABC[ContentMeta],
    owned: bool = True,
    name: Optional[str] = None
):
    if name is None:
        content_meta_name = content_meta_store.get_meta().name
        if content_meta_name.endswith('_content_meta'):
            name = content_meta_name[:-len('_content_meta')]
    upload_store_factory = DefaultStoreFactory(upload_store)
    if owned:
        upload_store_factory = OwnedUploadStoreFactory(upload_store_factory)
    data_store = DataStore(
        name=name,
        content_meta_store_factory=DefaultStoreFactory(content_meta_store),
        upload_store_factory=upload_store_factory,
        chunk_store_factory=DefaultStoreFactory(chunk_store),
        upload_store=upload_store,
        authorizer=dataclasses.replace(get_default_authorizer(), aud=f'data_url_{name}'),
        secured_upload_path=f"/data/{name.replace('_', '-')}",
        public_download_path="/data/" + name.replace('_', '-') + "/{key}",
    )
    return data_store
