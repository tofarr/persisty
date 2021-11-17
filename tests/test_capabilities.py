import dataclasses
from unittest import TestCase

from old.persisty import Capabilities, ALL_CAPABILITIES, NO_CAPABILITIES, CapabilitiesMarshaller

A = Capabilities(**{f.name: (i & 1 == 1) for i, f in enumerate(dataclasses.fields(Capabilities))})
B = Capabilities(**{f.name: (i & 1 == 0) for i, f in enumerate(dataclasses.fields(Capabilities))})


class TestEdit(TestCase):

    def test_and(self):
        assert ALL_CAPABILITIES & ALL_CAPABILITIES == ALL_CAPABILITIES
        assert ALL_CAPABILITIES & NO_CAPABILITIES == NO_CAPABILITIES
        assert NO_CAPABILITIES & NO_CAPABILITIES == NO_CAPABILITIES
        assert A & A == A
        assert B & B == B
        assert A & B == NO_CAPABILITIES

    def test_or(self):
        assert ALL_CAPABILITIES | ALL_CAPABILITIES == ALL_CAPABILITIES
        assert ALL_CAPABILITIES | NO_CAPABILITIES == ALL_CAPABILITIES
        assert NO_CAPABILITIES | NO_CAPABILITIES == NO_CAPABILITIES
        assert A | A == A
        assert B | B == B
        assert A | B == ALL_CAPABILITIES

    def test_add(self):
        assert ALL_CAPABILITIES + ALL_CAPABILITIES == ALL_CAPABILITIES
        assert ALL_CAPABILITIES + NO_CAPABILITIES == ALL_CAPABILITIES
        assert NO_CAPABILITIES + NO_CAPABILITIES == NO_CAPABILITIES
        assert A + A == A
        assert B + B == B
        assert A + B == ALL_CAPABILITIES

    def test_sub(self):
        assert ALL_CAPABILITIES - ALL_CAPABILITIES == NO_CAPABILITIES
        assert ALL_CAPABILITIES - NO_CAPABILITIES == ALL_CAPABILITIES
        assert NO_CAPABILITIES - NO_CAPABILITIES == NO_CAPABILITIES
        assert A - A == NO_CAPABILITIES
        assert B - B == NO_CAPABILITIES
        assert A - B == A
        assert B - A == B

    def test_eq(self):
        for f in dataclasses.fields(Capabilities):
            assert Capabilities(**{f.name: True}) != NO_CAPABILITIES

    def test_lt(self):
        assert NO_CAPABILITIES < ALL_CAPABILITIES
        assert ALL_CAPABILITIES > NO_CAPABILITIES

    def test_marshaller(self):
        marshaller = CapabilitiesMarshaller()
        assert marshaller.load([]) == NO_CAPABILITIES
        assert marshaller.load([f.name for f in dataclasses.fields(Capabilities)]) == ALL_CAPABILITIES
        assert marshaller.dump(NO_CAPABILITIES) == []
        assert set(marshaller.dump(ALL_CAPABILITIES)) == {f.name for f in dataclasses.fields(Capabilities)}
