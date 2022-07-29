from unittest import TestCase

from persisty.access_control.authorization import Authorization
from persisty.context import new_default_persisty_context
from persisty.obj_storage.stored import get_storage_meta
from tests.fixtures.number_name import NumberName


class TestDefaultContext(TestCase):
    def test_default_persisty_context(self):
        context = new_default_persisty_context()
        # Mock authorization will do for now...
        authorization = Authorization(None, frozenset(), None, None)
        meta_storage = context.get_obj_meta_storage(authorization)

        # Create the storage for NumberNames...
        storage_meta = get_storage_meta(NumberName)
        meta_storage.create(storage_meta)

        number_name_storage = context.get_storage(storage_meta.name)


        print(context)
