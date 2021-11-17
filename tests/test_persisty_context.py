from os import environ
from unittest import TestCase

from persisty import __version__
from old.persisty import PersistyError
from old.persisty.persisty_context import PersistyContext, get_default_persisty_context, PERSISTY_CONTEXT
from old.persisty.storage.in_mem_storage import in_mem_storage
from tests.fixtures.items import Band


class TestPersistyContext(TestCase):

    def test_version(self):
        assert __version__ is not None

    def test_get_default_persisty_context(self):
        setattr(old.persisty.persisty_context, '_default_context', None)
        environ[PERSISTY_CONTEXT] = f'{__name__}.{MyPersistyContext.__name__}'
        persisty_context = get_default_persisty_context()
        assert isinstance(persisty_context, MyPersistyContext)
        del environ[PERSISTY_CONTEXT]
        setattr(old.persisty.persisty_context, '_default_context', None)

    def test_get_missing_storage(self):
        with self.assertRaises(PersistyError):
            PersistyContext().get_storage(Band)

    def test_get_storages(self):
        persisty_context = PersistyContext()
        storage = in_mem_storage(Band)
        persisty_context.register_storage(storage)
        assert list(persisty_context.get_storages()) == [storage]


class MyPersistyContext(PersistyContext):

    def __init__(self):
        super().__init__()
