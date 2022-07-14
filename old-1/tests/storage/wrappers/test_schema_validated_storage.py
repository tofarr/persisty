from dataclasses import fields

from marshy import get_default_context
from schemey.datetime_schema import DatetimeSchema
from schemey.uuid_schema import UuidSchema

from persisty.attr.attr import attr_from_field
from persisty.attr.attr_mode import AttrMode
from persisty.edit import Edit
from persisty.edit_type import EditType
from schemey.any_of_schema import optional_schema
from schemey.object_schema import ObjectSchema
from schemey.property_schema import PropertySchema
from schemey.schema_error import SchemaError
from schemey.string_schema import StringSchema

from persisty.key_config.attr_key_config import UuidKeyConfig
from persisty.storage.in_mem.in_mem_storage import InMemStorage, in_mem_storage
from persisty.storage.in_mem.in_mem_storage_context import InMemStorageContext
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrappers.schema_validated_storage import schema_validated_storage
from persisty.storage.wrappers.timestamped_storage import with_timestamps
from tests.fixtures.item_types import Node, Band, Member, Tag, NodeTag
from tests.fixtures.storage_data import populate_data
from tests.storage.in_mem.test_in_mem_storage import TestInMemStorage


class TestSchemaValidatedStorage(TestInMemStorage):

    def create_storage_context(self):
        storage_context = InMemStorageContext()
        storage_context.register_storage(schema_validated_storage(in_mem_storage(Band)))
        storage_context.register_storage(schema_validated_storage(in_mem_storage(Member)))
        storage_context.register_storage(schema_validated_storage(with_timestamps(in_mem_storage(Tag))))
        storage_context.register_storage(schema_validated_storage(with_timestamps(in_mem_storage(Node))))
        storage_context.register_storage(schema_validated_storage(with_timestamps(in_mem_storage(NodeTag))))
        return storage_context

    def setUp(self):
        meta = StorageMeta(
            name=Node.__name__,
            key_config=UuidKeyConfig(key_generation=AttrMode.EXCLUDED),
            attrs=tuple(attr_from_field(f) for f in fields(Node))
        )
        marshaller = get_default_context().get_marshaller(Node)
        self.storage = schema_validated_storage(InMemStorage(meta, marshaller))
        self.storage_context = self.create_storage_context()
        populate_data(self.storage_context)

    def test_schema_for_create(self):
        create_schema = ObjectSchema[Node](Node, tuple((
            PropertySchema('title', StringSchema(), True),
            PropertySchema('parent_id', optional_schema(UuidSchema()), False),
            PropertySchema('created_at', optional_schema(DatetimeSchema())),
            PropertySchema('updated_at', optional_schema(DatetimeSchema()))
        )))
        assert self.storage.schema_for_create == create_schema

    def test_schema_for_update(self):
        update_schema = ObjectSchema[Node](Node, tuple((
            PropertySchema('id', UuidSchema(), True),
            PropertySchema('title', StringSchema(), True),
            PropertySchema('parent_id', optional_schema(UuidSchema()), False),
            PropertySchema('created_at', optional_schema(DatetimeSchema())),
            PropertySchema('updated_at', optional_schema(DatetimeSchema()))
        )))
        assert self.storage.schema_for_update == update_schema

    def test_create(self):
        node = Node('Everything is awesome')
        key = self.storage.create(node)
        read: Node = self.storage.read(key)
        assert read.id is not None

    def test_create_disallow(self):
        # noinspection PyTypeChecker
        node = Node(None)
        with self.assertRaises(SchemaError):
            self.storage.create(node)

    def test_update(self):
        node = Node('Everything is awesome')
        self.storage.storage.create(node)
        self.storage.update(node)

    def test_update_disallow(self):
        node = Node('Everything is awesome')
        self.storage.storage.create(node)
        node.title = None
        with self.assertRaises(SchemaError):
            self.storage.update(node)

    def create_issues(self):
        expected = [Node(f'issue_{i}') for i in range(1, 4)]
        self.storage.storage.edit_all(Edit(EditType.CREATE, None, i) for i in expected)

    def test_edit_all_create_disallow(self):
        self.create_issues()
        # noinspection PyTypeChecker
        edits = [
            Edit(EditType.DESTROY, 'issue_2'),
            Edit(EditType.CREATE, None, Node(None, f'issue_4')),
        ]
        with self.assertRaises(SchemaError):
            self.storage.edit_all(edits)
        assert self.storage.read('issue_2') is None

    def test_edit_all_update_disallow(self):
        self.create_issues()
        # noinspection PyTypeChecker
        edits = [
            Edit(EditType.UPDATE, None, Node(None, f'issue_3')),
        ]
        with self.assertRaises(SchemaError):
            self.storage.edit_all(edits)
