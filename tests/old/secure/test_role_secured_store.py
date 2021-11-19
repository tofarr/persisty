from unittest import TestCase

from old.persisty import READ_ONLY, ALL_CAPABILITIES
from persisty.errors import PersistyError
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.security.current_user import set_current_user
from old.persisty.secure import RoleCheck
from old.persisty.secure.role_secured_storage import role_secured_storage
from old.persisty.secure.role_storage_filter import RoleStorageFilter
from old.persisty.storage.in_mem_storage import in_mem_storage
from tests.old.fixtures.data import setup_bands
from tests.old.fixtures.items import Band
from tests.old.secure.test_current_user import User

ADMIN = User('admin', ('admin',))
USER = User('user', ('user',))


class TestRoleSecuredStorage(TestCase):

    @staticmethod
    def get_role_secured_storage():
        """ Create a storage that only admins can edit """
        storage = in_mem_storage(Band)
        setup_bands(storage)
        storage = role_secured_storage(storage, [
            RoleStorageFilter(RoleCheck(['user']), READ_ONLY, AttrFilter('year_formed', AttrFilterOp.gt, 1900)),
            RoleStorageFilter(RoleCheck(['admin']), ALL_CAPABILITIES)
        ])
        return storage

    def test_create_by_admin(self):
        set_current_user(ADMIN)
        try:
            storage = self.get_role_secured_storage()
            jefferson = Band('jefferson_airplane', 'Jefferson Airplane')
            storage.create(jefferson)
            assert jefferson == storage.read('jefferson_airplane')
        finally:
            set_current_user(None)

    def test_create_by_user(self):
        set_current_user(USER)
        try:
            storage = self.get_role_secured_storage()
            jefferson = Band('jefferson_airplane', 'Jefferson Airplane')
            with self.assertRaises(PersistyError):
                storage.create(jefferson)
            assert storage.read('jefferson_airplane') is None
        finally:
            set_current_user(None)

    def test_read_disallowed(self):
        storage = self.get_role_secured_storage()
        with self.assertRaises(PersistyError):
            storage.read('beatles')
