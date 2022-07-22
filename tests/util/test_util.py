from typing import Union
from unittest import TestCase

from dataclasses import dataclass

from persisty.util import to_base64, from_base64, secure_hash, dataclass_to_params
from persisty.util.undefined import Undefined, UNDEFINED


class TestUtil(TestCase):
    def test_base64_round_trip(self):
        msg = "This is a test"
        b64 = to_base64(msg)
        self.assertNotEqual(b64, msg)
        decoded = from_base64(b64)
        self.assertEqual(decoded, msg)

    def test_base64_round_trip_dict(self):
        payload = dict(msg="This is a test")
        b64 = to_base64(payload)
        decoded = from_base64(b64)
        self.assertEqual(decoded, payload)

    def test_base64_round_trip_none(self):
        payload = None
        b64 = to_base64(payload)
        decoded = from_base64(b64)
        self.assertEqual(decoded, payload)

    def test_secure_hash(self):
        payload = dict(msg="This is a test")
        hash = secure_hash(payload)
        payload["msg"] += "!"
        updated_hash = secure_hash(payload)
        self.assertNotEqual(hash, updated_hash)

    def test_dataclass_to_params(self):
        @dataclass
        class Foo:
            title: Union[str, Undefined, type(None)] = UNDEFINED

        self.assertEqual({}, dataclass_to_params(Foo()))
        self.assertEqual(dict(title="Foobar"), dataclass_to_params(Foo("Foobar")))
