from dataclasses import dataclass
from typing import Optional, List, Iterator, Type

from marshy import ExternalType
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.marshaller_context import MarshallerContext

from schemey.schema_abc import SchemaABC, T
from schemey.schema_error import SchemaError


@dataclass(frozen=True)
class HasManyPagedSchema(SchemaABC, T):
    has_many_entity: str

    def get_schema_errors(self, item: T, current_path: Optional[List[str]] = None) -> Iterator[SchemaError]:
        if item is None:
            yield SchemaError(current_path, 'missing_dependency')

    @classmethod
    def __marshaller_factory__(cls, marshaller_context: MarshallerContext):
        return _BelongsToSchemaMarshaller()


class _HasManySchemaMarshaller(MarshallerABC[BelongsToSchema]):

    def load(self, item: ExternalType) -> BelongsToSchema:
        return BelongsToSchema(item.get('belongsToKey'), belongs_to_entity=item.get('belongsToEntity'))

    def dump(self, item: BelongsToSchema) -> ExternalType:
        return dict(type='object', belongsToKey=item.belongs_to_key, belongsToType=item.belongs_to_type)