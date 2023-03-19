from unittest import TestCase

from persisty.attr.generator.defaults import get_default_generator_for_create


class TestDefaults(TestCase):
    def test_str(self):
        creatable, generator = get_default_generator_for_create("id", str)
        self.assertFalse(creatable)
        self.assertEqual("1", generator.transform(None))
        self.assertEqual("2", generator.transform(None))

    def test_int(self):
        creatable, generator = get_default_generator_for_create("id", int)
        self.assertFalse(creatable)
        self.assertEqual(1, generator.transform(None))
        self.assertEqual(2, generator.transform(None))
