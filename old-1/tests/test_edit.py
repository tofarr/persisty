from unittest import TestCase

from persisty.edit import Edit
from persisty.edit_type import EditType
from tests.fixtures.storage_data import BANDS


class TestEdit(TestCase):

    def test_init_no_edit_type(self):
        with self.assertRaises(ValueError):
            Edit(EditType.CREATE)
        with self.assertRaises(ValueError):
            Edit(EditType.UPDATE)
        with self.assertRaises(ValueError):
            Edit(EditType.DESTROY)

    def test_init_create_invalid(self):
        with self.assertRaises(ValueError):
            Edit(EditType.CREATE, BANDS[0].id, BANDS[0])
        with self.assertRaises(ValueError):
            Edit(EditType.CREATE, BANDS[0].id)

    def test_init_update_invalid(self):
        with self.assertRaises(ValueError):
            Edit(EditType.UPDATE, BANDS[0].id, BANDS[0])
        with self.assertRaises(ValueError):
            Edit(EditType.UPDATE, BANDS[0].id)

    def test_init_destroy_invalid(self):
        with self.assertRaises(ValueError):
            Edit(EditType.DESTROY, BANDS[0].id, BANDS[0])
        with self.assertRaises(ValueError):
            Edit(EditType.DESTROY, item=BANDS[0])
