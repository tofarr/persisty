from typing import Optional
from unittest import TestCase
from uuid import UUID

from servey.security.authorization import Authorization

from persisty.factory.default_store_factory import DefaultStoreFactory
from persisty.factory.owned_store_factory import OwnedStoreFactory
from persisty.impl.mem.mem_store import MemStore
from persisty.store_meta import get_meta
from persisty.stored import stored


@stored
class Message:
    id: UUID
    owner: str
    text: str


class TestOwnedStoreFactory(TestCase):
    def test_getters(self):
        meta = get_meta(Message)
        store = MemStore(meta)
        factory = OwnedStoreFactory(DefaultStoreFactory(store), "owner")
        self.assertEqual(meta, factory.get_meta())
        subject_1 = Authorization("subject-1", frozenset(), None, None)
        subject_2 = Authorization("subject-2", frozenset(), None, None)
        subject_1_store = factory.create(subject_1)
        subject_2_store = factory.create(subject_2)
        msg_1 = subject_1_store.create(
            Message(id=UUID("f13dc535-9beb-488b-95d9-7f19f0d0a147"), text="Some test")
        )
        msg_2 = subject_2_store.create(
            Message(
                id=UUID("078709c0-0a7c-4a06-9599-b52511a6afa8"), text="Some other text"
            )
        )
        self.assertEqual("subject-1", msg_1.owner)
        self.assertEqual("subject-2", msg_2.owner)
        msg_2_updated = subject_1_store.update(msg_2)
        self.assertIsNone(msg_2_updated)
        msg_2_read = subject_2_store.read(msg_2.id)
        self.assertEqual(msg_2, msg_2_read)

    def test_create_actions(self):
        meta = get_meta(Message)
        store = MemStore(meta)
        factory = OwnedStoreFactory(DefaultStoreFactory(store), "owner")
        actions = factory.create_actions()
        action_names = {a.name for a in actions}
        expected_action_names = {
            "message_count",
            "message_create",
            "message_delete",
            "message_edit_batch",
            "message_read",
            "message_read_batch",
            "message_search",
            "message_update",
        }
        self.assertEqual(expected_action_names, action_names)
        self.assertEqual([], list(factory.create_routes()))
