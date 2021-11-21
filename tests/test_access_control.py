import dataclasses
from unittest import TestCase

from marshy import get_default_context

from persisty.access_control.access_control import AccessControl, ALL_ACCESS, NO_ACCESS

A = AccessControl(**{f.name: (i & 1 == 1) for i, f in enumerate(dataclasses.fields(AccessControl))})
B = AccessControl(**{f.name: (i & 1 == 0) for i, f in enumerate(dataclasses.fields(AccessControl))})


class TestEdit(TestCase):

    def test_and(self):
        assert ALL_ACCESS & ALL_ACCESS == ALL_ACCESS
        assert ALL_ACCESS & NO_ACCESS == NO_ACCESS
        assert NO_ACCESS & NO_ACCESS == NO_ACCESS
        assert A & A == A
        assert B & B == B
        assert A & B == NO_ACCESS

    def test_or(self):
        assert ALL_ACCESS | ALL_ACCESS == ALL_ACCESS
        assert ALL_ACCESS | NO_ACCESS == ALL_ACCESS
        assert NO_ACCESS | NO_ACCESS == NO_ACCESS
        assert A | A == A
        assert B | B == B
        assert A | B == ALL_ACCESS

    def test_add(self):
        assert ALL_ACCESS + ALL_ACCESS == ALL_ACCESS
        assert ALL_ACCESS + NO_ACCESS == ALL_ACCESS
        assert NO_ACCESS + NO_ACCESS == NO_ACCESS
        assert A + A == A
        assert B + B == B
        assert A + B == ALL_ACCESS

    def test_sub(self):
        assert ALL_ACCESS - ALL_ACCESS == NO_ACCESS
        assert ALL_ACCESS - NO_ACCESS == ALL_ACCESS
        assert NO_ACCESS - NO_ACCESS == NO_ACCESS
        assert A - A == NO_ACCESS
        assert B - B == NO_ACCESS
        assert A - B == A
        assert B - A == B

    def test_eq(self):
        for f in dataclasses.fields(AccessControl):
            assert AccessControl(**{f.name: True}) != NO_ACCESS

    def test_lt(self):
        assert NO_ACCESS < ALL_ACCESS
        assert ALL_ACCESS > NO_ACCESS

    def test_marshaller(self):
        marshaller = get_default_context().get_marshaller(AccessControl)
        assert marshaller.load([]) == NO_ACCESS
        assert marshaller.load([f.name for f in dataclasses.fields(AccessControl)]) == ALL_ACCESS
        assert marshaller.dump(NO_ACCESS) == []
        assert set(marshaller.dump(ALL_ACCESS)) == {f.name for f in dataclasses.fields(AccessControl)}
