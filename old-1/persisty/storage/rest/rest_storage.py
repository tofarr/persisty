from dataclasses import dataclass, field
import json
from typing import Optional, Iterator, Type
from urllib import request
from urllib import parse

from marshy import get_default_context
from marshy.marshaller_context import MarshallerContext

from persisty.item_filter.item_filter_abc import ItemFilterABC
from persisty.key_config.attr_key_config import UuidKeyConfig
from persisty.key_config.key_config_abc import KeyConfigABC
from persisty.page import Page
from persisty.storage.storage_abc import StorageABC, T
from persisty.storage.storage_filter import StorageFilter
from persisty.storage.storage_meta import StorageMeta


@dataclass(frozen=True)
class RestStorageABC(StorageABC):
    root_url: str
    item_type: Type[T]
    marshaller_context: MarshallerContext = field(default_factory=get_default_context)
    key_config: KeyConfigABC = UuidKeyConfig()
    page_size: int = 20

    @property
    def meta(self) -> StorageMeta:
        with request.urlopen(f'{self.root_url}?meta=1') as url:
            json_data = json.loads(url.read().decode())
            meta = self.marshaller_context.load(StorageMeta, json_data)
            return meta

    def _encode_item(self, item: T):
        dumped = self.marshaller_context.dump(item, self.item_type)
        json_data = json.dumps(dumped)
        encoded_data = json_data.encode('utf-8')
        return encoded_data

    def _decode_item(self, response):
        json_data = json.loads(response.read().decode())
        loaded = self.marshaller_context.load(self.item_type, json_data)
        return loaded

    def create(self, item: T) -> str:
        req = request.Request(self.root_url, data=self._encode_item(item))
        with request.urlopen(req) as response:
            return self._decode_item(response)

    def read(self, key: str) -> Optional[T]:
        with request.urlopen(f'{self.root_url}/{key}') as response:
            return self._decode_item(response)

    def update(self, item: T) -> T:
        key = self.key_config.get_key(item)
        url = f'{self.root_url}/{key}'
        data = self._encode_item(item)
        req = request.Request(url, data=data, method='PUT')
        with request.urlopen(req) as response:
            return self._decode_item(response)

    def destroy(self, key: str) -> bool:
        url = f'{self.root_url}/{key}'
        req = request.Request(url, method='DELETE')
        with request.urlopen(req) as response:
            result = response.code == 200
            return result

    def search(self, storage_filter: Optional[StorageFilter[T]] = None) -> Iterator[T]:
        page_key = None
        while True:
            page = self.paged_search(storage_filter, page_key, self.page_size)
            yield from page.items
            page_key = page.next_page_key
            if page_key is None:
                return

    def count(self, item_filter: Optional[ItemFilterABC[T]] = None) -> int:
        url_params = dict(count=1)
        if item_filter:
            dumped = self.marshaller_context.dump(item_filter, ItemFilterABC)
            dumped_str = json.dumps(dumped)
            url_params['item_filter_json'] = dumped_str
        url = f'{self.root_url}?{parse.urlencode(url_params)}'
        with request.urlopen(url) as response:
            count = int(response.read().decode())
            return count

    def paged_search(self, storage_filter: Optional[StorageFilter[T]] = None, page_key: Optional[str] = None,
                     limit: int = 20) -> Page[T]:
        url_params = {}
        if storage_filter:
            dumped = self.marshaller_context.dump(storage_filter, StorageFilter)
            dumped_str = json.dumps(dumped)
            url_params['storage_filter_json'] = dumped_str
        url = f'{self.root_url}?{parse.urlencode(url_params)}'
        with request.urlopen(url) as response:
            json_data = json.loads(response.read().decode())
            loaded = self.marshaller_context.load(Page[self.item_type], json_data)
            return loaded
