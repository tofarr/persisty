from unittest import TestCase

from persisty.capabilities import READ_ONLY
from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.errors import PersistyError
from persisty.schema.object_schema import ObjectSchema
from persisty.schema.optional_schema import OptionalSchema
from persisty.schema.property_schema import PropertySchema
from persisty.schema.schema_error import SchemaError
from persisty.schema.string_schema import StringSchema
from persisty.store.capability_filter_store import CapabilityFilterStore
from persisty.store.in_mem_store import in_mem_store
from persisty.store.schema_store import schema_store
from persisty.store_schemas import StoreSchemas
from tests.fixtures.items import Issue


class TestSchemaStore(TestCase):

    def setUp(self):
        self.store = schema_store(in_mem_store(Issue))

    def test_schemas(self):
        created_at_schema = PropertySchema('created_at', OptionalSchema(StringSchema()))
        updated_at_schema = PropertySchema('updated_at', OptionalSchema(StringSchema()))
        read_schema = ObjectSchema[Issue](tuple((
            PropertySchema('id', StringSchema(min_length=1)),
            PropertySchema('title', StringSchema()),
            created_at_schema,
            updated_at_schema
        )))
        create_schema = ObjectSchema[Issue](tuple((
            PropertySchema('id', OptionalSchema(StringSchema(min_length=1))),
            PropertySchema('title', StringSchema()),
            created_at_schema,
            updated_at_schema
        )))
        update_schema = ObjectSchema[Issue](tuple((
            PropertySchema('id', StringSchema(min_length=1)),
            PropertySchema('title', StringSchema()),
            created_at_schema,
            updated_at_schema
        )))
        expected = StoreSchemas[Issue](create_schema, update_schema, read_schema)
        assert self.store.schemas == expected

    def test_create(self):
        issue = Issue('Something is wrong')
        key = self.store.create(issue)
        read: Issue = self.store.read(key)
        assert read.id is not None

    def test_create_disallow(self):
        issue = Issue(None)
        with self.assertRaises(SchemaError):
            self.store.create(issue)

    def test_update(self):
        issue = Issue('Something is wrong')
        self.store.store.create(issue)
        self.store.update(issue)

    def test_update_disallow(self):
        issue = Issue('Something is wrong')
        self.store.store.create(issue)
        issue.title = None
        with self.assertRaises(SchemaError):
            self.store.update(issue)

    def create_issues(self):
        expected = [Issue(f'Issue {i}', f'issue_{i}') for i in range(1, 4)]
        self.store.store.edit_all(Edit(EditType.CREATE, None, i) for i in expected)

    def test_edit_all_create_disallow(self):
        self.create_issues()
        edits = [
            Edit(EditType.DESTROY, 'issue_2'),
            Edit(EditType.CREATE, None, Issue(None, f'issue_4')),
        ]
        with self.assertRaises(SchemaError):
            self.store.edit_all(edits)
        assert self.store.read('issue_2') is None

    def test_edit_all_update_disallow(self):
        self.create_issues()
        edits = [
            Edit(EditType.UPDATE, None, Issue(None, f'issue_3')),
        ]
        with self.assertRaises(SchemaError):
            self.store.edit_all(edits)

    def test_read_only(self):
        store = schema_store(CapabilityFilterStore(in_mem_store(Issue), READ_ONLY))
        assert store.name == 'Issue'
        read_schema = ObjectSchema[Issue](tuple((
            PropertySchema('id', StringSchema(min_length=1)),
            PropertySchema('title', StringSchema()),
            PropertySchema('created_at', OptionalSchema(StringSchema())),
            PropertySchema('updated_at', OptionalSchema(StringSchema()))
        )))
        expected = StoreSchemas(None, None, read_schema)
        assert store.schemas == expected
        with self.assertRaises(PersistyError):
            store.create(Issue('Issue 4', 'issue_4'))
