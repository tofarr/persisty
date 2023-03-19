from dataclasses import dataclass
from unittest import TestCase
from uuid import uuid4

from boto3.dynamodb.conditions import Attr as DynAttr

from persisty.attr.attr_filter import AttrFilter
from persisty.attr.attr_filter_op import AttrFilterOp
from persisty.util import UNDEFINED


@dataclass
class FooBar:
    foo: int
    bar: str


class TestAttrFilter(TestCase):
    def test_match(self):
        attr_filter = AttrFilter("foo", AttrFilterOp.eq, 10)
        self.assertTrue(attr_filter.match(FooBar(10, "bar")))
        self.assertFalse(attr_filter.match(FooBar(11, "bar")))

    def test_match_starts_with(self):
        attr_filter = AttrFilter("bar", AttrFilterOp.startswith, "zap")
        self.assertTrue(attr_filter.match(FooBar(10, "zapbang")))
        self.assertFalse(attr_filter.match(FooBar(10, "zopbang")))

    def test_match_type_error(self):
        attr_filter = AttrFilter("bar", AttrFilterOp.lt, "zap")
        self.assertFalse(attr_filter.match(FooBar(10, uuid4())))

    def test_build_filter_expression_starts_with(self):
        attr_filter = AttrFilter("foo", AttrFilterOp.startswith, "bar")
        filter_expression, handled = attr_filter.build_filter_expression(tuple())
        self.assertTrue(handled)
        self.assertEqual(DynAttr("foo").begins_with("bar"), filter_expression)
        attr_filter = AttrFilter("foo", AttrFilterOp.startswith, 10)
        filter_expression, handled = attr_filter.build_filter_expression(tuple())
        self.assertTrue(handled)
        self.assertEqual(DynAttr("foo").begins_with(10), filter_expression)

    def test_build_filter_expression_exists(self):
        attr_filter = AttrFilter("foo", AttrFilterOp.exists, UNDEFINED)
        filter_expression, handled = attr_filter.build_filter_expression(tuple())
        self.assertTrue(handled)
        self.assertEqual(DynAttr("foo").exists(), filter_expression)

    def test_build_filter_expression_not_exists(self):
        attr_filter = AttrFilter("foo", AttrFilterOp.not_exists, UNDEFINED)
        filter_expression, handled = attr_filter.build_filter_expression(tuple())
        self.assertTrue(handled)
        self.assertEqual(DynAttr("foo").not_exists(), filter_expression)

    def test_build_filter_expression_oneof(self):
        attr_filter = AttrFilter("bar", AttrFilterOp.oneof, ["zap", "bang"])
        filter_expression, handled = attr_filter.build_filter_expression(tuple())
        self.assertFalse(handled)
        self.assertIsNone(filter_expression)
