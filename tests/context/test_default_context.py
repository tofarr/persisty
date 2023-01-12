from unittest import TestCase

from persisty.access_control.authorization import Authorization
from persisty.context import new_default_persisty_context
from persisty.obj_store.stored import get_meta
from tests.fixtures.number_name import NumberName


class TestDefaultContext(TestCase):
    def test_default_persisty_context(self):
        context = new_default_persisty_context()
        # Mock authorization will do for now...
        authorization = Authorization(None, frozenset(), None, None)
        meta_store = context.get_obj_meta_store(authorization)

        # Create the store for NumberNames...
        store_meta = get_meta(NumberName)
        meta_store.create(store_meta)

        number_name_store = context.get_store(store_meta.name)

        print(context)
