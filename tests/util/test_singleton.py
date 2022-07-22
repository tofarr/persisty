from unittest import TestCase

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
