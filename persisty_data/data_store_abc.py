from abc import ABC, abstractmethod
from typing import Optional, Iterator

from servey.action.action import action, Action, get_action
from servey.security.authorization import Authorization
from servey.trigger.web_trigger import WEB_GET

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty_data.upload_form import UploadForm

_ROUTE = "starlette.routing.Route"
_CONTENT_META = "persisty_data.upload.ContentMeta"
_UPLOAD = "persisty_data.upload.Upload"
_CHUNK = "persisty_data.chunk.Chunk"



class DataStoreABC(ABC):
    """ Factory for stores related to data and uploads, which are all linked internally. """

    @abstractmethod
    def get_name(self) -> str:
        """ Get a name for this factory (Used in actions) """

    @abstractmethod
    def create_routes(self) -> Iterator[_ROUTE]:
        """
        Create routes for this factory. In hosted mode, uploads and downloads may go through python.
        In a lambda environment, uploads and downloads should be based on S3 and not go through the python
        environment.
        """

    def create_actions(self) -> Iterator[Action]:
        """ add actions for this factory """
        yield self.create_get_upload_config_action()
        yield self.create_get_download_url_action()

    @abstractmethod
    def get_content_meta_store_factory(self) -> StoreFactoryABC[_CONTENT_META]:
        """
        Get the store for content meta for the data store
        """

    @abstractmethod
    def get_upload_store_factory(self) -> StoreFactoryABC[_UPLOAD]:
        """
        Get the store for uploads for the data store
        """

    @abstractmethod
    def get_chunk_store_factory(self) -> StoreFactoryABC[_CHUNK]:
        """
        Get the store for chunks for the data store
        """

    @abstractmethod
    def url_for_download(self, authorization: Optional[Authorization], key: str) -> Optional[str]:
        """
        Create a url which may be used to download this resource using the authorization given. Assumes that any
        required actions have been linked to the url by `add_actions`. Depending on the implementation, url may have
        an expiration timestamp
        """

    @abstractmethod
    def form_for_upload(
        self,
        authorization: Authorization,
        key: Optional[str]
    ) -> UploadForm:
        """
        Create a url which may be used to upload this resource using the authorization given. Assumes that any
        required actions have been linked to the url by `add_actions`. Depending on the implementation, url may have
        an expiration timestamp
        """

    def create_get_upload_config_action(self) -> Action:
        @action(
            name=f"{self.get_name()}_form_for_upload",
            triggers=WEB_GET,
            description="Create a url which may be used to upload files"
        )
        def url_for_upload(authorization: Optional[Authorization], key: str) -> Optional[UploadForm]:
            result = self.form_for_upload(authorization, key)
            return result

        return get_action(url_for_upload)

    def create_get_download_url_action(self) -> Action:
        @action(
            name=f"{self.get_name()}_url_for_download",
            triggers=WEB_GET,
            description="Create a url which may be used to download files"
        )
        def url_for_download(authorization: Optional[Authorization], key: str) -> Optional[str]:
            result = self.url_for_download(authorization, key)
            return result

        return get_action(url_for_download)

    def all_urls_for_download(
        self,
        authorization: Optional[Authorization],
        keys: Iterator[str]
    ) -> Iterator[Optional[str]]:
        for key in keys:
            yield self.url_for_download(authorization, key)
