import itertools
from dataclasses import dataclass
from typing import Iterable, Optional, Union, Sized, TypeVar, Iterator, List
from uuid import uuid4

from marshy.marshaller.marshaller_abc import MarshallerABC

from lambsync.persistence import dynamo
from lambsync.persistence.capabilities import Capabilities, ALL
from lambsync.persistence.dynamo.dynamo_index import DynamoIndex
from lambsync.persistence.dynamo.dynamo_search_factory import DynamoSearchFactory
from lambsync.persistence.dynamo.dynamo_table import DynamoTable
from lambsync.persistence.edit import Edit
from lambsync.persistence.edit_type import EditType
from lambsync.persistence.page import Page
from lambsync.persistence.persistence_error import PersistenceError
from lambsync.persistence.store_abc import StoreABC
from lambsync.persistence.util import secure_hash, from_base64, to_base64

T = TypeVar('T')
F = TypeVar('F')


@dataclass(frozen=True)
class DynamoStore(StoreABC[T, F]):
    table: DynamoTable
    marshaller: MarshallerABC[T]
    dynamo_search_factory: DynamoSearchFactory[F] = DynamoSearchFactory[F]()
    projected_attributes: Union[Iterable[DynamoIndex], Sized, None] = None
    batch_size: int = 100

    def get_capabilities(self) -> Capabilities:
        return ALL

    def get_key(self, item: T) -> str:
        dumped = self.marshaller.dump(item)
        key = self.table.primary_index.key_to_str(dumped)
        return key

    def create(self, item: T) -> str:
        dumped = self.marshaller.dump(item)
        primary_index = self.table.primary_index
        pk = primary_index.pk
        if pk not in dumped:
            dumped[pk] = str(uuid4())
        key_dict = primary_index.isolate_key(dumped)
        dynamo.create(self.table.name, key_dict, dumped)
        key = primary_index.key_to_str(key_dict)
        return key

    def read(self, key: str) -> Optional[T]:
        table = self.table
        key_dict = table.primary_index.str_to_key(key)
        item = dynamo.read(table.name, key_dict, self.projected_attributes)
        if not item:
            return None
        loaded = self.marshaller.load(item)
        return loaded

    def read_all(self, keys: Iterator[str], error_on_missing: bool = True) -> Iterator[T]:
        while True:
            batch_keys = list(itertools.islice(keys, self.batch_size))
            if not batch_keys:
                return
            yield from self._read_all_internal(batch_keys, error_on_missing)

    def _read_all_internal(self, batch_keys: List[str], error_on_missing: bool):
        primary_index = self.table.primary_index
        keys = (primary_index.str_to_key(k) for k in batch_keys)
        items = dynamo.read_all(self.table.name, keys, self.projected_attributes)
        items_by_key = {primary_index.key_to_str(i): i for i in items}
        for k in batch_keys:
            item = items_by_key.get(k)
            if item is None and error_on_missing:
                raise PersistenceError(f'missing_item:{k}')
            loaded = self.marshaller.load(item)
            yield loaded

    def update(self, item: T) -> T:
        key_dict = self.table.primary_index.isolate_key(item)
        dumped = self.marshaller.dump(item)
        dynamo.update(self.table.name, key_dict, dumped)
        return item

    def destroy(self, key: str) -> bool:
        key_dict = self.table.primary_index.str_to_key(key)
        attributes = dynamo.destroy(self.table.name, key_dict)
        return bool(attributes)

    def search(self, search_filter: Optional[F] = None) -> Iterator[T]:
        dynamo_search = self.dynamo_search_factory.create(self.table, search_filter)
        raw_results = dynamo_search.search()
        results = iter(self.marshaller.load(r) for r in raw_results)
        return results

    def count(self, search_filter: Optional[F] = None) -> int:
        dynamo_search = self.dynamo_search_factory.create(self.table, search_filter)
        count = dynamo_search.count()
        return count

    def paged_search(self,
                         search_filter: Optional[F] = None,
                         page_key: str = None,
                         limit: int = 20
                         ) -> Page[T]:
        exclusive_start_key = None
        search_filter_hash = None
        if page_key:
            decoded_page_key = from_base64(page_key)
            exclusive_start_key = decoded_page_key['exclusive_start_key']
            search_filter_hash = secure_hash(search_filter)
            if search_filter_hash != decoded_page_key['search_filter_hash']:
                raise PersistenceError(f'search_filter_changed')
        dynamo_search = self.dynamo_search_factory.create(self.table, search_filter)
        raw_results = dynamo_search.search(exclusive_start_key)
        raw_results = list(itertools.islice(raw_results, limit))
        next_page_key = None
        if len(raw_results) == limit:
            last_result = raw_results[-1]
            exclusive_start_key = self.table.primary_index.key_to_str(last_result)
            if dynamo_search.index_name:
                secondary_index = next(i for i in self.table.global_secondary_indexes
                                       if i.name == dynamo_search.index_name)
                exclusive_start_key = {**exclusive_start_key, **secondary_index.key_for_item(last_result)}
            search_filter_hash = search_filter_hash or secure_hash(search_filter)
            next_page_key = to_base64(dict(exclusive_start_key=exclusive_start_key,
                                           search_filter_hash=search_filter_hash))
        items = (self.marshaller.load(r) for r in raw_results)
        return Page(items, next_page_key)

    def bulk_edit(self, edits: Iterator[Edit[T]]):
        while True:
            batch_edits = list(itertools.islice(edits, self.batch_size))
            if not batch_edits:
                return
            self._edit_all_internal(batch_edits)

    def _edit_all_internal(self, edits: List[Edit]):
        table = self.table
        primary_index = table.primary_index
        # A sanity check first that all items for create and update
        puts = [e for e in edits if e.edit_type in [EditType.CREATE, EditType.UPDATE] and e.key]
        items = dynamo.read_all(table.name, (primary_index.str_to_key(e.key) for e in edits))
        existing_put_keys = {primary_index.key_to_str(i) for i in items}
        for p in puts:
            if p.edit_type == EditType.CREATE:
                if p.key in existing_put_keys:
                    raise PersistenceError(f'create_key_already_exists:{p.key}')
            elif p.edit_type == EditType.UPDATE:
                if p.key not in existing_put_keys:
                    raise PersistenceError(f'update_key_does_not_exist:{p.key}')

        with dynamo.dynamodb_table(table.name).batch_writer() as batch:
            for edit in edits:
                if edit.edit_type in [EditType.CREATE, EditType.UPDATE]:
                    dumped = self.marshaller.dump(edit.value)
                    batch.put_item(Item=dumped)
                elif edit.edit_type == EditType.DESTROY:
                    batch.delete_item(Key=edit.key)
