from unittest import TestCase

from persisty.access_control.access_control import READ_ONLY, ALL_ACCESS
from persisty.errors import PersistyError
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.security.current_user import set_current_user
from persisty.security.role_check import RoleCheck
from persisty.security.role_secured_storage import role_secured_storage
from persisty.security.role_storage_filter import RoleStorageFilter
from persisty.storage.in_mem.in_mem_storage import in_mem_storage
from persisty.storage.storage_context_abc import get_default_storage_context
from tests.fixtures.item_types import Band
from tests.fixtures.storage_data import BANDS
from tests.security.test_current_user import User

ADMIN = User('admin', ('admin',))
USER = User('user', ('user',))


class TestRoleSecuredStorage(TestCase):

    @staticmethod
    def get_role_secured_storage():
        storage_context = get_default_storage_context()
        """ Create a storage that only admins can edit """
        storage = in_mem_storage(Band)
        for band in BANDS:
            storage.create(band)
        storage = role_secured_storage(storage, [
            RoleStorageFilter(RoleCheck(['user']), READ_ONLY, AttrFilter('year_formed', AttrFilterOp.gt, 1900)),
            RoleStorageFilter(RoleCheck(['admin']), ALL_ACCESS)
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
