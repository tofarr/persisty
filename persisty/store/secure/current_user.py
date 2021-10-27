import threading
from typing import TypeVar

U = TypeVar('U')
_user_context = threading.local()
_user_context.current_user = None


def get_current_user() -> U:
    return _user_context.current_user


def set_current_user(user: U):
    _user_context.current_user = user
