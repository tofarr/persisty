from datetime import datetime
from unittest import TestCase
from uuid import uuid4

from schemey.datetime_schema import DatetimeSchema
from schemey.uuid_schema import UuidSchema

from persisty.attr.attr import Attr
from persisty.attr.attr_access_control import AttrAccessControl, REQUIRED, OPTIONAL
from persisty.attr.attr_mode import AttrMode
from persisty.cache_control.secure_hash_cache_control import SecureHashCacheControl
from persisty.cache_control.timestamp_cache_control import TimestampCacheControl
from persisty.edit import Edit
from schemey.any_of_schema import optional_schema
from schemey.string_schema import StringSchema

from persisty.edit_type import EditType
from persisty.storage.in_mem.in_mem_storage import in_mem_storage
from persisty.storage.storage_filter import storage_filter_from_dataclass
from persisty.storage.storage_meta import StorageMeta
from persisty.storage.wrappers.timestamped_storage import TimestampedStorage, with_timestamps
from tests.fixtures.item_types import Node, NodeFilter


class TestTimestampStorage(TestCase):

    def setUp(self):
        self.storage: TimestampedStorage = with_timestamps(in_mem_storage(Node))

    def test_meta(self):
        meta = self.storage.meta
        expected = StorageMeta(
            name='Node',
            attrs=(
                Attr('title', StringSchema(), REQUIRED),
                Attr('id', UuidSchema(), AttrAccessControl(
                     update_mode=AttrMode.REQUIRED,
                     read_mode=AttrMode.REQUIRED,
                     search_mode=AttrMode.REQUIRED)),
                Attr('parent_id', UuidSchema(), OPTIONAL),
                Attr('created_at', DatetimeSchema(), AttrAccessControl(
                    create_mode=AttrMode.EXCLUDED,
                    update_mode=AttrMode.EXCLUDED,
                    read_mode=AttrMode.REQUIRED,
                    search_mode=AttrMode.REQUIRED)),
                Attr('updated_at', DatetimeSchema(), AttrAccessControl(
                    create_mode=AttrMode.EXCLUDED,
                    update_mode=AttrMode.EXCLUDED,
                    read_mode=AttrMode.REQUIRED,
                    search_mode=AttrMode.REQUIRED)),
            ),
            cache_control=TimestampCacheControl(cache_control=SecureHashCacheControl())
        )
        assert meta == expected

    def test_create(self):
        node = Node('Something is wrong')
        assert node.created_at is None
        assert node.updated_at is None
        key = self.storage.create(node)
        read: Node = self.storage.read(key)
        assert read.id is not None
        assert read.created_at is not None
        assert read.updated_at is not None

    def test_update(self):
        ten_seconds_ago = datetime.fromtimestamp(datetime.now().timestamp() - 10)
        node = Node('Something is wrong', None, None, created_at=ten_seconds_ago, updated_at=ten_seconds_ago)
        self.storage.storage.create(node)
        assert node.updated_at == ten_seconds_ago
        self.storage.update(node)
        assert node.updated_at > ten_seconds_ago

    def test_edit_all(self):
        ten_seconds_ago = datetime.fromtimestamp(datetime.now().timestamp() - 10)
        expected = [Node(f'Node {i}', uuid4(), None, ten_seconds_ago, ten_seconds_ago) for i in range(1, 4)]
        self.storage.storage.edit_all(Edit(EditType.CREATE, None, i) for i in expected)
        edits = [
            Edit(EditType.CREATE, None, Node(f'Node 4', uuid4())),
            Edit(EditType.UPDATE, None, expected[0]),
            Edit(EditType.DESTROY, expected[1].id)
        ]
        self.storage.edit_all(edits)
        storage_filter = storage_filter_from_dataclass(NodeFilter(sort='id'), Node)
        nodes = list(self.storage.search(storage_filter))
        ids = {node.id for node in nodes}
        assert ids == {expected[0].id, expected[2].id, edits[0].item.id}
        for node in nodes:
            assert node.created_at is not None
            assert node.updated_at is not None
            if node.id == expected[0].id:
                assert node.created_at < node.updated_at
            else:
                assert node.created_at == node.updated_at
