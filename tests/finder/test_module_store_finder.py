from dataclasses import dataclass
from unittest import TestCase
from unittest.mock import patch

from persisty.factory.default_store_factory import DefaultStoreFactory
from persisty.finder.module_store_finder import ModuleStoreFinder
from persisty.finder.store_finder_abc import find_stores, find_store_factories


class TestModuleStoreFinder(TestCase):
    def test_find_stores(self):
        finder = ModuleStoreFinder("tests.finder")
        stores = list(finder.find_stores())
        store_names = {s.get_meta().name for s in stores}
        expected_store_names = {"message"}
        self.assertEqual(store_names, expected_store_names)

    def test_find_store_factories(self):
        finder = ModuleStoreFinder("tests.finder")
        factories = list(finder.find_store_factories())
        store_names = {s.get_meta().name for s in factories}
        expected_store_names = {"message"}
        self.assertEqual(store_names, expected_store_names)
        factory = next(iter(factories))
        self.assertTrue(isinstance(factory, DefaultStoreFactory))

    def test_globals(self):
        @dataclass
        class MyModuleStoreFinder(ModuleStoreFinder):
            root_module_name: str = "tests.finder"

        with patch(
            "persisty.finder.store_finder_abc.get_impls",
            return_value=[MyModuleStoreFinder],
        ):
            expected_store_names = {"message"}
            stores = list(find_stores())
            factories = list(find_store_factories())
            self.assertEqual({s.get_meta().name for s in stores}, expected_store_names)
            self.assertEqual(
                {s.get_meta().name for s in factories}, expected_store_names
            )
