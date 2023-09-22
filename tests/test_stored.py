import math
from unittest import TestCase

from persisty.errors import PersistyError
from persisty.stored import stored


@stored
class Vector2D:
    id: int
    x: float
    y: float

    def get_magnitude(self) -> float:
        return math.sqrt(self.x * self.x + self.y * self.y)


@stored
class Vector3D(Vector2D):
    z: float

    def get_magnitude(self) -> float:
        return math.sqrt(self.x * self.x + self.y * self.y + self.z * self.z)


class TestFunctionsOnStored(TestCase):

    def test_function_on_stored(self):
        vector = Vector2D(x=3, y=4)
        self.assertEquals(vector.get_magnitude(), 5.0)

    def test_inheritance_on_stored(self):
        vector = Vector3D(x=3, y=4, z=5)
        self.assertEquals(vector.get_magnitude(), math.sqrt(3*3 + 4*4 + 5*5))

    def test_property_on_stored(self):
        with self.assertRaises(PersistyError):
            @stored
            class InvalidVector:
                id: int

                @property
                def properties_are_not_allowed(self) -> str:
                    return "nope"
