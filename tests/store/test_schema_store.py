from typing import Optional, List, Iterator, Dict
from unittest import TestCase

from schemey.ref_schema import RefSchema
from schemey.with_defs_schema import WithDefsSchema

from old.persisty import READ_ONLY
from persisty.edit import Edit
from old.persisty import EditType
from old.persisty import PersistyError
from schemey.any_of_schema import optional_schema
from schemey.object_schema import ObjectSchema
from schemey.property_schema import PropertySchema
from schemey.schema_abc import SchemaABC, T
from schemey.schema_context import schema_for_type, SchemaContext
from schemey.schema_error import SchemaError
from schemey.string_schema import StringSchema
from old.persisty.storage.capability_filter_storage import CapabilityFilterStorage
from old.persisty.storage.in_mem_storage import in_mem_storage
from old.persisty.storage.schema_storage import schema_storage
from old.persisty.storage_schemas import StorageSchemas, schemas_for_type
from tests.fixtures.items import Issue, Band


class TestSchemaStorage(TestCase):

    def setUp(self):
        self.storage = schema_storage(in_mem_storage(Issue))

    def test_schemas(self):
        created_at_schema = PropertySchema('created_at', optional_schema(StringSchema()))
        updated_at_schema = PropertySchema('updated_at', optional_schema(StringSchema()))
        read_schema = WithDefsSchema({'Issue': ObjectSchema[Issue](tuple((
            PropertySchema('id', StringSchema(min_length=1), True),
            PropertySchema('title', StringSchema(), True),
            created_at_schema,
            updated_at_schema
        )))}, RefSchema('Issue'))
        create_schema = WithDefsSchema({'Issue': ObjectSchema[Issue](tuple((
            PropertySchema('id', optional_schema(StringSchema(min_length=1))),
            PropertySchema('title', StringSchema(), True),
            created_at_schema,
            updated_at_schema
        )))}, RefSchema('Issue'))
        update_schema = WithDefsSchema({'Issue': ObjectSchema[Issue](tuple((
            PropertySchema('id', StringSchema(min_length=1), True),
            PropertySchema('title', StringSchema(), True),
            created_at_schema,
            updated_at_schema
        )))}, RefSchema('Issue'))
        expected = StorageSchemas[Issue](create_schema, update_schema, read_schema, read_schema)
        assert self.storage.schemas == expected

    def test_create(self):
        issue = Issue('Something is wrong')
        key = self.storage.create(issue)
        read: Issue = self.storage.read(key)
        assert read.id is not None

    def test_create_disallow(self):
        # noinspection PyTypeChecker
        issue = Issue(None)
        with self.assertRaises(SchemaError):
            self.storage.create(issue)

    def test_update(self):
        issue = Issue('Something is wrong')
        self.storage.storage.create(issue)
        self.storage.update(issue)

    def test_update_disallow(self):
        issue = Issue('Something is wrong')
        self.storage.storage.create(issue)
        issue.title = None
        with self.assertRaises(SchemaError):
            self.storage.update(issue)

    def create_issues(self):
        expected = [Issue(f'Issue {i}', f'issue_{i}') for i in range(1, 4)]
        self.storage.storage.edit_all(Edit(EditType.CREATE, None, i) for i in expected)

    def test_edit_all_create_disallow(self):
        self.create_issues()
        # noinspection PyTypeChecker
        edits = [
            Edit(EditType.DESTROY, 'issue_2'),
            Edit(EditType.CREATE, None, Issue(None, f'issue_4')),
        ]
        with self.assertRaises(SchemaError):
            self.storage.edit_all(edits)
        assert self.storage.read('issue_2') is None

    def test_edit_all_update_disallow(self):
        self.create_issues()
        # noinspection PyTypeChecker
        edits = [
            Edit(EditType.UPDATE, None, Issue(None, f'issue_3')),
        ]
        with self.assertRaises(SchemaError):
            self.storage.edit_all(edits)

    def test_read_only(self):
        storage = schema_storage(CapabilityFilterStorage(in_mem_storage(Issue), READ_ONLY))
        assert storage.name == 'Issue'
        read_schema = WithDefsSchema({
            'Issue': ObjectSchema[Issue](tuple((
                PropertySchema('id', StringSchema(min_length=1), True),
                PropertySchema('title', StringSchema(), True),
                PropertySchema('created_at', optional_schema(StringSchema())),
                PropertySchema('updated_at', optional_schema(StringSchema()))
            )))
        }, RefSchema('Issue'))
        expected = StorageSchemas(None, None, read_schema, read_schema)
        assert storage.schemas == expected
        with self.assertRaises(PersistyError):
            storage.create(Issue('Issue 4', 'issue_4'))

    def test_schema_for_type(self):

        class CustomSchema(SchemaABC[T]):

            def get_schema_errors(self, item: T, current_path: Optional[List[str]] = None) -> Iterator[SchemaError]:
                """ Never called"""

        class NotADataclass:
            @classmethod
            def __schema_factory__(cls, schema_context: SchemaContext, defs: Dict[str, SchemaABC]):
                return CustomSchema()

        schema = schema_for_type(NotADataclass)
        assert isinstance(schema, CustomSchema)
        assert schemas_for_type(NotADataclass) == StorageSchemas(schema, schema, schema, schema)

    def test_schemas_for_type_no_key(self):
        schema = schema_for_type(Band)
        schemas = schemas_for_type(Band, None)
        assert schemas == StorageSchemas(schema, schema, schema, schema)
