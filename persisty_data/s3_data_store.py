import dataclasses
from itertools import islice
from typing import Optional, Iterator

from botocore.exceptions import ClientError

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.errors import PersistyError
from persisty.result_set import ResultSet
from persisty.search_filter.include_all import INCLUDE_ALL
from persisty.search_filter.search_filter_abc import SearchFilterABC
from persisty.search_order.search_order import SearchOrder
from persisty.store_meta import StoreMeta
from persisty_data.data_item_abc import DataItemABC, DATA_ITEM_META
from persisty_data.data_store_abc import DataStoreABC
from persisty_data.s3_client import get_s3_client
from persisty_data.s3_data_item import S3DataItem


class S3DataStore(DataStoreABC):
    store_meta: StoreMeta
    bucket_name: str
    max_size: int = 1024 * 1024 * 100

    def get_meta(self) -> StoreMeta:
        meta = self.store_meta
        if not meta:
            meta = self.store_meta = dataclasses.replace(DATA_ITEM_META, name=self.bucket_name)
        return meta

    def create(self, item: DataItemABC) -> Optional[S3DataItem]:
        if self.read(item.key):
            raise PersistyError(f"existing_value:{item.key}")
        destination = S3DataItem(
            bucket_name=self.bucket_name,
            key=item.key,
            max_size=self.max_size,
            _content_type=item.content_type,
        )
        destination.copy_data_from(item)
        return destination

    def read(self, key: str) -> Optional[S3DataItem]:
        item = S3DataItem(
            bucket_name=self.bucket_name,
            key=key,
            max_size=self.max_size,
        )
        item.load_meta()
        if item.updated_at:
            return item

    def _update(self, key: str, item: S3DataItem, updates: DataItemABC) -> Optional[S3DataItem]:
        item.copy_data_from(updates)
        return item

    def delete(self, key: str) -> bool:
        try:
            response = get_s3_client().delete_object(Bucket=self.bucket_name, Key=key)
            return response['DeleteMarker']
        except ClientError:
            return False

    def _delete(self, key: str, item: S3DataItem) -> bool:
        return self.delete(key)

    def count(self, search_filter: SearchFilterABC[DataItemABC] = INCLUDE_ALL) -> int:
        # Unfortunately, s3 does not seem to have a count function. :(
        result = sum(1 for _ in self.search_all(search_filter=search_filter))
        return result

    def search(
        self,
        search_filter: SearchFilterABC[DataItemABC] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[DataItemABC]] = None,
        page_key: Optional[str] = None,
        limit: Optional[int] = None,
    ) -> ResultSet[S3DataItem]:
        if limit:
            assert limit <= 1000
        else:
            limit = 1000
        if self._is_native_search(search_filter, search_order):
            return self._search_native(search_filter, page_key, limit)
        else:
            results = self._search_all_local(search_filter, search_order)
            while True:
                result = next(results)
                if result.key == page_key:
                    break
            results = list(islice(results, limit))
            next_page_key = None
            if len(results) == limit:
                next_page_key = results[-1].key
            return ResultSet(
                results=results,
                next_page_key=next_page_key
            )

    def search_all(
        self,
        search_filter: SearchFilterABC[DataItemABC] = INCLUDE_ALL,
        search_order: Optional[SearchOrder[DataItemABC]] = None,
    ) -> Iterator[S3DataItem]:
        if self._is_native_search(search_filter, search_order):
            return super().search_all(search_filter, search_order)
        return self._search_all_local(search_filter, search_order)

    @staticmethod
    def _is_native_search(
        search_filter: SearchFilterABC[DataItemABC],
        search_order: Optional[SearchOrder[DataItemABC]]
    ) -> bool:
        if search_order and search_order.orders:
            if len(search_order.orders) > 1:
                return False
            if search_order.orders[0].attr != 'key' or search_order.orders[0].desc:
                return False
        if search_filter != INCLUDE_ALL:
            if (
                    not isinstance(search_filter, AttrFilter)
                    or search_filter.name != 'key'
                    or search_filter.op != AttrFilterOp.startswith
            ):
                return False
        return True

    def _search_native(
        self,
        search_filter: SearchFilterABC[DataItemABC],
        page_key: Optional[str],
        limit: Optional[int]
    ) -> ResultSet[S3DataItem]:
        prefix = ""
        if search_filter is not INCLUDE_ALL:
            prefix = search_filter.value
        response = get_s3_client().list_objects_v2(
            Bucket=self.bucket_name,
            MaxKeys=limit,
            Prefix=prefix,
            ContinuationToken=page_key
        )
        results = [
            S3DataItem(
                bucket_name=self.bucket_name,
                key=c['Key'],
                max_size=self.max_size,
                _etag=c['ETag'],
                _updated_at=c['LastModified']
            )
            for c in (response.get('Contents') or [])
        ]
        result_set = ResultSet(
            next_page_key=response.get('NextContinuationToken'),
            results=results
        )
        return result_set

    def _search_all_local(
        self,
        search_filter: SearchFilterABC[DataItemABC],
        search_order: Optional[SearchOrder[DataItemABC]]
    ) -> Iterator[S3DataItem]:
        results = self._load_all()
        attrs = self.store_meta.attrs
        results = (r for r in results if search_filter.match(r, attrs))
        if search_order:
            results = list(results)
            search_order.sort(results)
            results = iter(results)
        return results

    def _load_all(self) -> Iterator[S3DataItem]:
        kwargs = dict(
            Bucket=self.bucket_name
        )
        while True:
            response = get_s3_client().list_objects_v2(**kwargs)
            for content in (response.get('Contents') or []):
                item = S3DataItem(
                    bucket_name=self.bucket_name,
                    key=content['Key'],
                    max_size=self.max_size,
                    _etag=content['ETag'],
                    _updated_at=content['LastModified']
                )
                yield item
            next_continuation_token = response.get('NextContinuationToken')
            if next_continuation_token:
                kwargs['NextContinuationToken'] = next_continuation_token
            else:
                return
