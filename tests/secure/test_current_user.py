from dataclasses import dataclass, field
from typing import Iterable
from unittest import TestCase
from uuid import uuid4

from persisty.security.current_user import get_current_user, set_current_user


@dataclass(frozen=True)
class User:
    id: str = field(default_factory=lambda: str(uuid4()))
    roles: Iterable[str] = field(default_factory=list)


class TestCurrentUser(TestCase):

    def test_current_user(self):
        assert get_current_user() is None
        user = User('me')
        set_current_user(user)
        assert get_current_user() == user
        set_current_user(None)
