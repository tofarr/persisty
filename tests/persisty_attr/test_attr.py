from enum import Enum
from unittest import TestCase

from schemey.schema import str_schema, int_schema

from persisty.attr.attr import Attr
from persisty.attr.attr_type import AttrType, attr_type
from persisty.util import UNDEFINED


class TestAttr(TestCase):
    def test_sanitize_type_str(self):
        attr = Attr("foo", AttrType.STR, str_schema())
        self.assertEqual("bar", attr.sanitize_type("bar"))

    def test_sanitize_type_int(self):
        attr = Attr("foo", AttrType.INT, int_schema())
        self.assertEqual(13, attr.sanitize_type(13))

    def test_sanitize_type_undefined(self):
        attr = Attr("foo", AttrType.STR, str_schema())
        self.assertIs(None, attr.sanitize_type(None))
        self.assertIs(UNDEFINED, attr.sanitize_type(UNDEFINED))

    def test_attr_type_enum(self):
        e = Enum("foobar", {"foo": 1, "bar": 2})
        self.assertEqual(AttrType.STR, attr_type(e))
