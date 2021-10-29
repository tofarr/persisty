from unittest import TestCase

from persisty.secure.role_check import RoleCheck
from tests.secure.test_current_user import User


class TestRoleCheck(TestCase):

    def test_any_allow(self):
        role_check = RoleCheck(('a', 'b', 'c'))
        assert role_check.match(User(roles=('a',)))
        assert role_check.match(User(roles=('b',)))
        assert role_check.match(User(roles=('c',)))
        assert role_check.match(User(roles=('a', 'd')))
        assert role_check.match(User(roles=('d', 'c')))
        assert role_check.match(User(roles=('a', 'b', 'c')))

    def test_any_disallow(self):
        role_check = RoleCheck(('a', 'b', 'c'))
        assert not role_check.match(User(roles=['d']))
        assert not role_check.match(User(roles=[]))

    def test_all_allow(self):
        role_check = RoleCheck(('a', 'b', 'c'), True)
        assert role_check.match(User(roles=('a', 'b', 'c')))
        assert role_check.match(User(roles=('a', 'b', 'c', 'd')))
        assert role_check.match(User(roles=('d', 'c', 'b', 'a', )))

    def test_all_disallow(self):
        role_check = RoleCheck(('a', 'b', 'c'), True)
        assert not role_check.match(User(roles=('a', 'b')))
        assert not role_check.match(User(roles=('a', 'c', 'd')))
        assert not role_check.match(User(roles=[]))
