from typing import List
from unittest import TestCase

from dataclasses import dataclass, field

from servey.util.singleton_abc import SingletonABC

from persisty.util.logify import logify


# noinspection PyMethodMayBeStatic
class Greeter(SingletonABC):
    def say_hello(self, name: str) -> str:
        return f"Hello {name}"

    def say_hello_kwargs(self, *, name: str) -> str:
        return f"Hello {name}"

    def say_hello_args(self, name: str, **_) -> str:
        return f"Hello {name}"

    def raise_an_error(self):
        raise TypeError("i_dont_like_you")


@dataclass
class MockLogger:
    infos: List = field(default_factory=list)

    def info(self, msg):
        self.infos.append(msg)


class TestLogify(TestCase):
    def test_logify(self):
        logger = MockLogger()
        # noinspection PyTypeChecker
        greeter = logify(Greeter(), logger=logger)
        greeter.say_hello("Developer")
        expected = MockLogger(
            [
                {
                    "name": "say_hello",
                    "time": 0,
                    "args": ("Developer",),
                }
            ]
        )
        self.assertEqual(expected, logger)

    def test_logify_args(self):
        logger = MockLogger()
        # noinspection PyTypeChecker
        greeter = logify(Greeter(), logger=logger)
        greeter.say_hello_args("Developer")
        expected = MockLogger(
            [
                {
                    "name": "say_hello_args",
                    "time": 0,
                    "args": ("Developer",),
                }
            ]
        )
        self.assertEqual(expected, logger)

    def test_logify_kwargs(self):
        logger = MockLogger()
        # noinspection PyTypeChecker
        greeter = logify(Greeter(), logger=logger)
        greeter.say_hello_kwargs(name="Developer")
        expected = MockLogger(
            [
                {
                    "name": "say_hello_kwargs",
                    "time": 0,
                    "kwargs": {"name": "Developer"},
                }
            ]
        )
        self.assertEqual(expected, logger)

    def test_logify_error(self):
        logger = MockLogger()
        # noinspection PyTypeChecker
        greeter = logify(Greeter(), logger=logger)
        try:
            greeter.raise_an_error() and self.assertTrue(False)
        except TypeError as e:
            self.assertEqual(str(e), "i_dont_like_you")  # He doesn't like you either!
        expected = MockLogger(
            [
                {
                    "name": "raise_an_error",
                    "time": 0,
                    "error": "i_dont_like_you",
                }
            ]
        )
        self.assertEqual(expected, logger)

    def test_logify_skip(self):
        logger = MockLogger()
        # noinspection PyTypeChecker
        greeter = logify(
            Greeter(), log_methods=frozenset(("say_hello_args",)), logger=logger
        )
        greeter.say_hello("Developer")
        expected = MockLogger()
        self.assertEqual(expected, logger)

    def test_logify_default(self):
        logger = MockLogger()
        # noinspection PyTypeChecker
        greeter = logify(Greeter())
        greeter.say_hello("Developer")
