from dataclasses import dataclass
from unittest import TestCase
from unittest.mock import patch

from persisty.finder.module_store_meta_finder import ModuleStoreMetaFinder
from persisty.finder.store_meta_finder_abc import find_store_meta


class TestModuleStoreFinder(TestCase):
    def test_find_stored(self):
        finder = ModuleStoreMetaFinder("tests.finder")
        stores = list(finder.find_store_meta())
        store_names = {s.name for s in stores}
        expected_store_names = {"message"}
        self.assertEqual(store_names, expected_store_names)

    def test_globals(self):
        @dataclass
        class MyModuleStoredFinder(ModuleStoreMetaFinder):
            root_module_name: str = "tests.finder"

        with patch(
            "persisty.finder.store_meta_finder_abc.get_impls",
            return_value=[MyModuleStoredFinder],
        ):
            expected_store_names = {"message"}
            store_meta = list(find_store_meta())
            self.assertEqual({s.name for s in store_meta}, expected_store_names)
