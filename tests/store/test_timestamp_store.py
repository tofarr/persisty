from datetime import datetime
from unittest import TestCase

from schemey.ref_schema import RefSchema
from schemey.with_defs_schema import WithDefsSchema

from persisty.edit import Edit
from old.persisty import EditType
from schemey.any_of_schema import optional_schema
from schemey.object_schema import ObjectSchema
from schemey.property_schema import PropertySchema
from schemey.string_format import StringFormat
from schemey.string_schema import StringSchema
from old.persisty2.storage_filter import storage_filter_from_dataclass
from old.persisty.storage.in_mem_storage import in_mem_storage
from old.persisty.storage.schema_storage import schema_storage
from old.persisty.storage.timestamp_storage import TimestampStorage, timestamp_storage
from old.persisty.storage_schemas import StorageSchemas, NO_SCHEMAS
from tests.fixtures.items import Issue, IssueFilter


class TestTimestampStorage(TestCase):

    def setUp(self):
        self.storage: TimestampStorage = timestamp_storage(in_mem_storage(Issue))

    def test_name(self):
        assert self.storage.name == self.storage.wrapped_storage.name

    def test_schemas_none(self):
        assert self.storage.schemas == NO_SCHEMAS

    def test_schemas_present(self):
        storage = timestamp_storage(schema_storage(self.storage))
        read_schema = WithDefsSchema({'Issue': ObjectSchema[Issue](tuple((
            PropertySchema('id', StringSchema(min_length=1), True),
            PropertySchema('title', StringSchema(), True),
            PropertySchema('created_at', StringSchema(format=StringFormat.DATE_TIME)),
            PropertySchema('updated_at', StringSchema(format=StringFormat.DATE_TIME))
        )))}, RefSchema('Issue'))
        create_schema = WithDefsSchema({'Issue': ObjectSchema[Issue](tuple((
            PropertySchema('id', optional_schema(StringSchema(min_length=1))),
            PropertySchema('title', StringSchema(), True),
        )))}, RefSchema('Issue'))
        update_schema = WithDefsSchema({'Issue': ObjectSchema[Issue](tuple((
            PropertySchema('id', StringSchema(min_length=1), True),
            PropertySchema('title', StringSchema(), True),
        )))}, RefSchema('Issue'))
        expected = StorageSchemas[Issue](create_schema, update_schema, read_schema)
        assert storage.schemas == expected

    def test_create(self):
        issue = Issue('Something is wrong')
        assert issue.created_at is None
        assert issue.updated_at is None
        key = self.storage.create(issue)
        read: Issue = self.storage.read(key)
        assert read.id is not None
        assert read.created_at is not None
        assert read.updated_at is not None

    def test_update(self):
        ten_seconds_ago = datetime.fromtimestamp(datetime.now().timestamp() - 10).isoformat()
        issue = Issue('Something is wrong', created_at=ten_seconds_ago, updated_at=ten_seconds_ago)
        self.storage.storage.create(issue)
        assert issue.updated_at == ten_seconds_ago
        self.storage.update(issue)
        assert issue.updated_at > ten_seconds_ago

    def test_edit_all(self):
        ten_seconds_ago = datetime.fromtimestamp(datetime.now().timestamp() - 10).isoformat()
        expected = [Issue(f'Issue {i}', f'issue_{i}', ten_seconds_ago, ten_seconds_ago) for i in range(1, 4)]
        self.storage.storage.edit_all(Edit(EditType.CREATE, None, i) for i in expected)
        edits = [
            Edit(EditType.CREATE, None, Issue(f'Issue 4', f'issue_4')),
            Edit(EditType.UPDATE, None, expected[0]),
            Edit(EditType.DESTROY, 'issue_2')
        ]
        self.storage.edit_all(edits)
        storage_filter = storage_filter_from_dataclass(IssueFilter(sort='id'), Issue)
        issues = list(self.storage.search(storage_filter))
        ids = {issue.id for issue in issues}
        assert ids == {'issue_1', 'issue_3', 'issue_4'}
        for issue in issues:
            assert issue.created_at is not None
            assert issue.updated_at is not None
            if issue.id == 'issue_1':
                assert issue.created_at < issue.updated_at
            else:
                assert issue.created_at == issue.updated_at
