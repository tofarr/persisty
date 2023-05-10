import os
from dataclasses import dataclass, field
from typing import Optional

import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth

from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.store.store_abc import StoreABC
from persisty.store_meta import T, StoreMeta


@dataclass
class OpenSearchDocStore(StoreABC):
    meta: StoreMeta
    host: str
    port: str = 443
    region: str = field(default_factory=lambda: os.environ.get("AWS_REGION"))
    _open_search: Optional[OpenSearch] = None

    def get_meta(self) -> StoreMeta:
        return self.meta

    def create(self, item: T) -> Optional[T]:
        pass

    def read(self, key: str) -> Optional[T]:
        pass

    def _update(self, key: str, item: T, updates: T) -> Optional[T]:
        pass

    def _delete(self, key: str, item: T) -> bool:
        pass

    def count(self, search_filter: SearchFilterABC[T] = INCLUDE_ALL) -> int:
        pass

    @property
    def open_search(self) -> OpenSearch:
        if self._open_search:
            return self._open_search
        session = boto3.Session()
        credentials = session.get_credentials().get_frozen_credentials()
        auth = AWSV4SignerAuth(credentials, self.region)
        open_search = self._open_search = OpenSearch(
            hosts=[{"host": self.host, "port": self.port}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection,
        )
        return open_search
