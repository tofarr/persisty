from unittest import TestCase

from marshy import dump, load

from persisty.util.singleton_abc import SingletonABC


class MySingleton(SingletonABC):
    pass


class TestSingleton(TestCase):
    def test_singleton(self):
        self.assertIs(MySingleton(), MySingleton())

    def test_eq(self):
        self.assertEqual(MySingleton(), MySingleton())

    def test_repr(self):
        self.assertEqual(str(MySingleton()), "MySingleton")

    def test_load_and_dump(self):
        dumped = dump(MySingleton())
        loaded = load(MySingleton, dumped)
        assert loaded is MySingleton()
