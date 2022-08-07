from typing import Optional
from unittest import TestCase

from pydantic import BaseModel
from schemey.schema import str_schema
from schemey.string_format import StringFormat

from persisty.integration.pydantic import field_for_pydantic


class TestPydantic(TestCase):
    def test_field_for_pydantic(self):
        field = field_for_pydantic(str_schema(str_format=StringFormat.EMAIL))

        class SomeModel(BaseModel):
            id: Optional[int]
            email: field

            class Config:
                arbitrary_types_allowed = True

        instance = SomeModel(id=123, email="foo@bar.com")
        self.assertEqual("foo@bar.com", instance.email)
        schema = SomeModel.schema()
        exected_schema = {
            "title": "SomeModel",
            "type": "object",
            "properties": {
                "id": {"title": "Id", "type": "integer"},
                "email": {"title": "Email", "type": "string", "format": "email"},
            },
            "required": ["email"],
        }
        # While testing, I found this did not work with some versions of pydantic due to it not
        # invoking the __modify_schema__ method
        self.assertEqual(exected_schema, schema)
