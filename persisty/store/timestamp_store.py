import dataclasses
from dataclasses import dataclass
from datetime import datetime
from typing import Iterator, TypeVar, Callable, Any

from persisty.cache_header import CacheHeader
from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.schema import SchemaABC
from persisty.schema.object_schema import ObjectSchema
from persisty.schema.optional_schema import remove_optional
from persisty.schema.property_schema import PropertySchema
from persisty.schema.string_format import StringFormat
from persisty.schema.string_schema import StringSchema
from persisty.store.store_abc import StoreABC
from persisty.store.wrapper_store_abc import WrapperStoreABC
from persisty.store_schemas import StoreSchemas

T = TypeVar('T')


def timestamp_str():
    return datetime.now().isoformat()


@dataclass(frozen=True)
class TimestampStore(WrapperStoreABC[T]):
    """ Store which updates timestamps on items prior to storage. """
    wrapped_store: StoreABC[T]
    op_schemas: StoreSchemas[T]
    created_at_attr: str = 'created_at'
    updated_at_attr: str = 'updated_at'
    timestamp: Callable[[], Any] = datetime.now

    @property
    def store(self):
        return self.wrapped_store

    @property
    def name(self) -> str:
        return self.store.name

    @property
    def schemas(self) -> StoreSchemas[T]:
        return self.op_schemas

    def get_cache_header(self, item: T) -> CacheHeader:
        cache_header = self.store.get_cache_header()
        updated_at = getattr(item, self.updated_at_attr)
        return CacheHeader(cache_header.cache_key, updated_at, cache_header.expire_at)

    def create(self, item: T) -> str:
        now = self.timestamp()
        setattr(item, self.created_at_attr, now)
        setattr(item, self.updated_at_attr, now)
        return self.store.create(item)

    def update(self, item: T) -> T:
        setattr(item, self.updated_at_attr, self.timestamp())
        return self.store.update(item)

    def edit_all(self, edits: Iterator[Edit[T]]):
        edits = (self._process_edit(e) for e in edits)
        return self.store.edit_all(edits)

    def _process_edit(self, edit):
        if edit.edit_type == EditType.CREATE:
            now = self.timestamp()
            setattr(edit.item, self.created_at_attr, now)
            setattr(edit.item, self.updated_at_attr, now)
        elif edit.edit_type == EditType.UPDATE:
            now = self.timestamp()
            setattr(edit.item, self.updated_at_attr, now)
        return edit


def timestamp_store(store: StoreABC[T],
                    created_at_attr: str = 'created_at',
                    updated_at_attr: str = 'updated_at',
                    timestamp: Callable = timestamp_str,
                    schemas: StoreSchemas[T] = None) -> TimestampStore[T]:
    if schemas is None:
        schemas = store.schemas
        schemas = StoreSchemas[T](
            create=_filter_timestamp_from_schema(schemas.create, created_at_attr, updated_at_attr),
            update=_filter_timestamp_from_schema(schemas.update, created_at_attr, updated_at_attr),
            read=_filter_read_schema(schemas.read, created_at_attr, updated_at_attr, timestamp)
        )
    return TimestampStore(store, schemas, created_at_attr, updated_at_attr, timestamp)


def _filter_timestamp_from_schema(schema: SchemaABC[T], created_at_attr: str, updated_at_attr: str):
    if not isinstance(schema, ObjectSchema):
        return schema
    property_schemas = (s for s in schema.property_schemas
                        if s.name not in (created_at_attr, updated_at_attr))
    return ObjectSchema(tuple(property_schemas))


def _filter_read_schema(schema: SchemaABC[T], created_at_attr: str, updated_at_attr: str, timestamp: Callable):
    if not isinstance(schema, ObjectSchema):
        return schema
    property_schemas = []
    for s in schema.property_schemas:
        if s.name in (created_at_attr, updated_at_attr):
            timestamp_schema = remove_optional(s.schema)
            if isinstance(timestamp_schema, StringSchema) and timestamp == timestamp_str:
                s = PropertySchema(s.name, dataclasses.replace(timestamp_schema, format=StringFormat.DATE_TIME))
        property_schemas.append(s)
    return ObjectSchema(tuple(property_schemas))
