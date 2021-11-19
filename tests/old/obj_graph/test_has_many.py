from typing import Iterable
from unittest import TestCase

from old.persisty.persisty_context import get_default_persisty_context
from persisty.errors import PersistyError
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.obj_graph import EntityABC
from persisty.obj_graph import OnDestroy
from persisty.obj_graph import HasMany
from old.persisty import Page
from old.persisty2.storage_filter import StorageFilter
from old.persisty.storage.in_mem_storage import in_mem_storage
from tests.old.fixtures.data import setup_bands, setup_members
from tests.old.fixtures.entities import BandEntity, MEMBER_ENTITY_CLASS, MemberEntity
from tests.old.fixtures.items import Band, Member

BEATLES_MEMBER_IDS = {'john', 'paul', 'george', 'ringo'}


class TestHasMany(TestCase):

    def setUp(self):
        persisty_context = get_default_persisty_context()
        band_storage = in_mem_storage(Band)
        setup_bands(band_storage)
        persisty_context.register_storage(band_storage)
        member_storage = in_mem_storage(Member)
        setup_members(member_storage)
        persisty_context.register_storage(member_storage)

    def test_read_missing(self):
        empty = BandEntity()
        assert empty.members is None

    def test_destroy_cascade(self):
        class CascadingBandEntity(EntityABC, Band):
            members: Iterable[MEMBER_ENTITY_CLASS] = \
                HasMany(foreign_key_attr='band_id', on_destroy=OnDestroy.CASCADE)
        self._do_destroy(CascadingBandEntity)
        storage = get_default_persisty_context().get_storage(Member)
        members = list(storage.read_all(iter(BEATLES_MEMBER_IDS), error_on_missing=False))
        assert members == [None, None, None, None]

    @staticmethod
    def _do_destroy(entity):
        beatles = entity.read('beatles')
        assert {m.id for m in beatles.members} == BEATLES_MEMBER_IDS
        beatles.destroy()

    def test_destroy_nullify(self):
        class NullifyingBandEntity(EntityABC, Band):
            members: Iterable[MEMBER_ENTITY_CLASS] = \
                HasMany(foreign_key_attr='band_id', on_destroy=OnDestroy.NULLIFY)
        self._do_destroy(NullifyingBandEntity)
        for m in get_default_persisty_context().get_storage(Member).read_all(iter(BEATLES_MEMBER_IDS)):
            assert m.band_id is None

    def test_destroy_invalid(self):
        with self.assertRaises(PersistyError):
            class NullifyingBandEntity(EntityABC, Band):
                members: Page[MEMBER_ENTITY_CLASS] = HasMany(foreign_key_attr='band_id', on_destroy=OnDestroy.NULLIFY)

            self._do_destroy(NullifyingBandEntity)

    def test_update_with_set(self):
        band = BandEntity.read('beatles')
        band.members = [*list(band.members)[:3], MemberEntity('pete', 'Pete Best', None, '1941-11-24')]
        band.save()
        assert BandEntity.read('beatles') == band
        members = list(MemberEntity.search(StorageFilter(AttrFilter('band_id', AttrFilterOp.eq, 'beatles'))))
        assert len(members) == 4
        assert band.members == members
        band.save()
        assert len(members) == 4
        assert band.members == members

    def test_create_with_set(self):
        band = BandEntity('white_stripes', 'The White Stripes', 1997)
        band.members = [
            MemberEntity('jack', 'Jack White', None, '1975-07-09'),
            MemberEntity('meg', 'Meg White', None, '1974-12-10')
        ]
        band.save()
        assert BandEntity.read('white_stripes') == band
        members = list(MemberEntity.search(StorageFilter(AttrFilter('band_id', AttrFilterOp.eq, 'white_stripes'))))
        assert len(members) == 2
        assert band.members == members

    def test_unresolve_all(self):
        band = BandEntity.read('beatles')
        band.members = [*list(band.members)[:3], MemberEntity('pete', 'Pete Best', None, '1941-11-24')]
        band.unresolve_all()
        assert {m.id for m in band.members} == {'john', 'paul', 'george', 'ringo'}
