from os import environ
from unittest import TestCase

import persisty
from persisty import __version__, PersistyContext
from persisty.errors import PersistyError
from persisty.store.in_mem_store import in_mem_store
from tests.fixtures.items import Band


class TestPersistyContext(TestCase):

    def test_version(self):
        assert __version__ is not None

    def test_get_persisty_context(self):
        setattr(persisty, '_persisty_context', None)
        environ[persisty.PERSISTY_CONTEXT] = f'{__name__}.{MyPersistyContext.__name__}'
        persisty_context = persisty.get_persisty_context()
        assert isinstance(persisty_context, MyPersistyContext)
        del environ[persisty.PERSISTY_CONTEXT]
        setattr(persisty, '_persisty_context', None)

    def test_get_missing_store(self):
        with self.assertRaises(PersistyError):
            PersistyContext().get_store(Band)

    def test_get_stores(self):
        persisty_context = PersistyContext()
        store = in_mem_store(Band)
        persisty_context.register_store(store)
        assert list(persisty_context.get_stores()) == [store]


class MyPersistyContext(PersistyContext):

    def __init__(self):
        super().__init__()
