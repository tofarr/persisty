from dataclasses import dataclass, field
from typing import Optional
from unittest import TestCase
from uuid import uuid4

from persisty.edit import Edit
from persisty.edit_type import EditType
from persisty.errors import PersistyError
from persisty.item_filter import AttrFilterOp, AttrFilter
from persisty.page import Page
from persisty.search_filter import SearchFilter
from persisty.secure.current_user import set_current_user
from persisty.secure.current_user_filter_store import CurrentUserFilterStore
from persisty.store.in_mem_store import in_mem_store
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


class TestCurrentUserFilterStore(TestCase):

    def setUp(self):
        set_current_user(WE)

    def tearDown(self):
        set_current_user(None)

    @staticmethod
    def get_store() -> CurrentUserFilterStore[Comment]:
        store = in_mem_store(Comment)
        store.edit_all(Edit(EditType.CREATE, item=c) for c in COMMENTS)
        store = CurrentUserFilterStore(store)
        return store

    def test_read(self):
        store = self.get_store()
        we_comment = next(c for c in COMMENTS if c.user_id == WE.id)
        assert store.read(we_comment.id) == we_comment
        other_comment = next(c for c in COMMENTS if c.user_id != WE.id)
        assert store.read(other_comment.id) is None

    def test_read_no_user(self):
        set_current_user(None)
        store = self.get_store()
        we_comment = next(c for c in COMMENTS if c.user_id == WE.id)
        with self.assertRaises(PersistyError):
            store.read(we_comment.id)

    def test_search(self):
        store = self.get_store()
        assert {c for c in store.search()} == {c for c in COMMENTS if c.user_id == WE.id}
        assert list(store.search(SearchFilter(AttrFilter('user_id', AttrFilterOp.eq, 'they')))) == []

    def test_paged_search(self):
        store = self.get_store()
        assert store.paged_search() == Page([c for c in COMMENTS if c.user_id == WE.id])

    def test_count(self):
        store = self.get_store()
        assert store.count() == 2
        assert store.count(AttrFilter('user_id', AttrFilterOp.eq, 'they')) == 0

    def test_create(self):
        store = self.get_store()
        comment = Comment('Another comment', THEY.id)
        store.create(comment)
        assert comment.user_id == WE.id
        assert comment == store.read(comment.id)

    def test_create_disallow(self):
        store = self.get_store()
        existing_comment = next(c for c in COMMENTS if c.user_id != WE.id)
        comment = Comment(**existing_comment.__dict__)
        with self.assertRaises(PersistyError):
            store.create(comment)
        assert existing_comment == store.wrapped_store.read(comment.id)

    def test_update(self):
        store = self.get_store()
        comment = Comment('Another comment')
        store.create(comment)
        assert comment.user_id == WE.id
        comment.text = 'Another comment - updated'
        comment.user_id = THEY.id
        store.update(comment)
        assert comment.user_id == WE.id
        assert comment == store.read(comment.id)

    def test_update_disallow(self):
        store = self.get_store()
        existing_comment = next(c for c in COMMENTS if c.user_id != WE.id)
        comment = Comment(**existing_comment.__dict__)
        with self.assertRaises(PersistyError):
            store.update(comment)
        assert existing_comment == store.wrapped_store.read(comment.id)

    def test_edit_all(self):
        store = self.get_store()
        comment = Comment('Another comment', THEY.id)
        store.edit_all([Edit(EditType.CREATE, item=comment)])
        assert comment.user_id == WE.id
        assert comment == store.read(comment.id)
