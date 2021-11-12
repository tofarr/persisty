from unittest import TestCase

from persisty.capabilities import READ_ONLY, ALL_CAPABILITIES
from persisty.errors import PersistyError
from persisty2.item_filter import AttrFilter, AttrFilterOp
from persisty.secure.current_user import set_current_user
from persisty.secure.role_check import RoleCheck
from persisty.secure.role_secured_store import role_secured_store
from persisty.secure.role_store_filter import RoleStoreFilter
from persisty.store.in_mem_store import in_mem_store
from tests.fixtures.data import setup_bands
from tests.fixtures.items import Band
from tests.secure.test_current_user import User

ADMIN = User('admin', ('admin',))
USER = User('user', ('user',))


class TestRoleSecuredStore(TestCase):

    @staticmethod
    def get_role_secured_store():
        """ Create a store that only admins can edit """
        store = in_mem_store(Band)
        setup_bands(store)
        store = role_secured_store(store, [
            RoleStoreFilter(RoleCheck(['user']), READ_ONLY, AttrFilter('year_formed', AttrFilterOp.gt, 1900)),
            RoleStoreFilter(RoleCheck(['admin']), ALL_CAPABILITIES)
        ])
        return store

    def test_create_by_admin(self):
        set_current_user(ADMIN)
        try:
            store = self.get_role_secured_store()
            jefferson = Band('jefferson_airplane', 'Jefferson Airplane')
            store.create(jefferson)
            assert jefferson == store.read('jefferson_airplane')
        finally:
            set_current_user(None)

    def test_create_by_user(self):
        set_current_user(USER)
        try:
            store = self.get_role_secured_store()
            jefferson = Band('jefferson_airplane', 'Jefferson Airplane')
            with self.assertRaises(PersistyError):
                store.create(jefferson)
            assert store.read('jefferson_airplane') is None
        finally:
            set_current_user(None)

    def test_read_disallowed(self):
        store = self.get_role_secured_store()
        with self.assertRaises(PersistyError):
            store.read('beatles')
