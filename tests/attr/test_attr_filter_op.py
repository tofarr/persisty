from unittest import TestCase

import marshy

from persisty.attr.attr_filter_op import (
    _lte,
    _endswith,
    _oneof,
    _exists,
    _not_exists,
    AttrFilterOp,
)
from persisty.util import UNDEFINED


class TestAttrFilterOp(TestCase):
    def test_lte(self):
        self.assertTrue(_lte(5, 6))
        self.assertTrue(_lte(6, 6))
        self.assertFalse(_lte(7, 6))

    def test_endswith(self):
        self.assertTrue(_endswith("foobar", "bar"))
        self.assertFalse(_endswith("foobarg", "bar"))

    def test_oneof(self):
        self.assertTrue(_oneof("a", ("a", "b", "c")))
        self.assertFalse(_oneof("d", ("a", "b", "c")))

    def test_exists(self):
        self.assertFalse(_exists(None, 1))
        self.assertFalse(_exists(UNDEFINED, 1))
        self.assertTrue(_exists("foobar", 1))

    def test_not_exists(self):
        self.assertTrue(_not_exists(None, 1))
        self.assertTrue(_not_exists(UNDEFINED, 1))
        self.assertFalse(_not_exists("foobar", 1))

    def test_attr_filter_op_marshaller(self):
        self.assertEqual("eq", marshy.dump(AttrFilterOp.eq))
        self.assertEqual("lte", marshy.dump(AttrFilterOp.lte))
        self.assertEqual(AttrFilterOp.eq, marshy.load(AttrFilterOp, "eq"))
        self.assertEqual(AttrFilterOp.lte, marshy.load(AttrFilterOp, "lte"))
