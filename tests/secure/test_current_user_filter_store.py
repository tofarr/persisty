from dataclasses import dataclass, field
from typing import Optional
from unittest import TestCase
from uuid import uuid4

from persisty.edit import Edit
from old.persisty import EditType
from old.persisty import PersistyError
from persisty.item_filter import AttrFilterOp, AttrFilter
from old.persisty import Page
from old.persisty2.storage_filter import StorageFilter
from persisty.security.current_user import set_current_user
from old.persisty.secure.current_user_filter_storage import CurrentUserFilterStorage
from old.persisty.storage.in_mem_storage import in_mem_storage
from tests.secure.test_current_user import User


@dataclass(unsafe_hash=True)
class Comment:
    text: str
    user_id: Optional[str] = None
    id: str = field(default_factory=lambda: str(uuid4()))


WE = User('we')
THEY = User('they')
COMMENTS = [
    Comment('comment 1', WE.id),
    Comment('comment 2', WE.id),
    Comment('comment 3', THEY.id),
    Comment('comment 4', THEY.id)
]


class TestCurrentUserFilterStorage(TestCase):

    def setUp(self):
        set_current_user(WE)

    def tearDown(self):
        set_current_user(None)

    @staticmethod
    def get_storage() -> CurrentUserFilterStorage[Comment]:
        storage = in_mem_storage(Comment)
        storage.edit_all(Edit(EditType.CREATE, item=c) for c in COMMENTS)
        storage = CurrentUserFilterStorage(storage)
        return storage

    def test_read(self):
        storage = self.get_storage()
        we_comment = next(c for c in COMMENTS if c.user_id == WE.id)
        assert storage.read(we_comment.id) == we_comment
        other_comment = next(c for c in COMMENTS if c.user_id != WE.id)
        assert storage.read(other_comment.id) is None

    def test_read_no_user(self):
        set_current_user(None)
        storage = self.get_storage()
        we_comment = next(c for c in COMMENTS if c.user_id == WE.id)
        with self.assertRaises(PersistyError):
            storage.read(we_comment.id)

    def test_search(self):
        storage = self.get_storage()
        assert {c for c in storage.search()} == {c for c in COMMENTS if c.user_id == WE.id}
        assert list(storage.search(StorageFilter(AttrFilter('user_id', AttrFilterOp.eq, 'they')))) == []

    def test_paged_search(self):
        storage = self.get_storage()
        assert storage.paged_search() == Page([c for c in COMMENTS if c.user_id == WE.id])

    def test_count(self):
        storage = self.get_storage()
        assert storage.count() == 2
        assert storage.count(AttrFilter('user_id', AttrFilterOp.eq, 'they')) == 0

    def test_create(self):
        storage = self.get_storage()
        comment = Comment('Another comment', THEY.id)
        storage.create(comment)
        assert comment.user_id == WE.id
        assert comment == storage.read(comment.id)

    def test_create_disallow(self):
        storage = self.get_storage()
        existing_comment = next(c for c in COMMENTS if c.user_id != WE.id)
        comment = Comment(**existing_comment.__dict__)
        with self.assertRaises(PersistyError):
            storage.create(comment)
        assert existing_comment == storage.wrapped_storage.read(comment.id)

    def test_update(self):
        storage = self.get_storage()
        comment = Comment('Another comment')
        storage.create(comment)
        assert comment.user_id == WE.id
        comment.text = 'Another comment - updated'
        comment.user_id = THEY.id
        storage.update(comment)
        assert comment.user_id == WE.id
        assert comment == storage.read(comment.id)

    def test_update_disallow(self):
        storage = self.get_storage()
        existing_comment = next(c for c in COMMENTS if c.user_id != WE.id)
        comment = Comment(**existing_comment.__dict__)
        with self.assertRaises(PersistyError):
            storage.update(comment)
        assert existing_comment == storage.wrapped_storage.read(comment.id)

    def test_edit_all(self):
        storage = self.get_storage()
        comment = Comment('Another comment', THEY.id)
        storage.edit_all([Edit(EditType.CREATE, item=comment)])
        assert comment.user_id == WE.id
        assert comment == storage.read(comment.id)
