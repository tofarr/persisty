from unittest import TestCase

from persisty.util import UNDEFINED
from persisty.util.undefined import Undefined


class TestUndefined(TestCase):
    def test_bool(self):
        self.assertFalse(bool(UNDEFINED))

    def test_hash(self):
        self.assertEqual(hash(UNDEFINED), hash(Undefined()))

    def test_singleton(self):
        self.assertIs(UNDEFINED, Undefined())
