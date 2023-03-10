from abc import abstractmethod
from typing import Optional, Iterator

from servey.action.action import Action, action, get_action
from servey.security.authorization import Authorization
from servey.trigger.web_trigger import WEB_GET

from persisty.factory.store_factory_abc import StoreFactoryABC
from persisty.finder.store_finder_abc import find_store_factories
from persisty_data.data_item_abc import DataItemABC
from persisty_data.upload_form import UploadForm


class DataStoreFactoryABC(StoreFactoryABC[DataItemABC]):

    @abstractmethod
    def get_upload_form(self, key: str, authorization: Optional[Authorization]) -> UploadForm:
        """
        Get the upload form parameters
        """

    @abstractmethod
    def get_download_url(self, key: str, authorization: Optional[Authorization]) -> str:
        """
        Get the download url
        """

    def get_all_download_urls(
            self,
            keys: Iterator[str],
            authorization: Optional[Authorization]
    ) -> Iterator[Optional[str]]:
        for key in keys:
            if key:
                yield self.get_download_url(key, authorization)
            else:
                yield None

    def get_upload_form_action(self) -> Action:

        @action(
            name=self.get_meta().name+'_get_upload_form',
            triggers=WEB_GET
        )
        def get_upload_form(key: str, authorization: Optional[Authorization]) -> Optional[UploadForm]:
            return self.get_upload_form(key, authorization)

        return get_action(get_upload_form)

    def get_download_url_action(self) -> Action:

        @action(
            name=self.get_meta().name + '_get_download_url',
            triggers=WEB_GET
        )
        def get_download_url(key: str, authorization: Optional[Authorization]) -> Optional[str]:
            return self.get_download_url(key, authorization)

        return get_action(get_download_url)

    def create_actions(self) -> Iterator[Action]:
        yield from super().create_actions()
        meta = self.get_meta().store_access
        if meta.creatable or meta.updatable:
            yield self.get_upload_form_action()
        if meta.readable:
            yield self.get_download_url_action()


def find_data_store_factories() -> Iterator[DataStoreFactoryABC]:
    yield from (s for s in find_store_factories() if isinstance(s, DataStoreFactoryABC))
