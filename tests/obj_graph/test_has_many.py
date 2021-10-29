from typing import List, Set
from unittest import TestCase

from persisty import get_persisty_context
from persisty.errors import PersistyError
from persisty.obj_graph.entity_abc import EntityABC
from persisty.obj_graph.resolver.before_destroy import OnDestroy
from persisty.obj_graph.resolver.has_many import HasMany
from persisty.page import Page
from persisty.store.in_mem_store import in_mem_store
from tests.fixtures.data import setup_bands, setup_members
from tests.fixtures.entities import BandEntity, MEMBER_ENTITY_CLASS
from tests.fixtures.items import Band, Member

BEATLES_MEMBER_IDS = {'john', 'paul', 'george', 'ringo'}


class TestHasMany(TestCase):

    def setUp(self):
        persisty_context = get_persisty_context()
        band_store = in_mem_store(Band)
        setup_bands(band_store)
        persisty_context.register_store(band_store)
        member_store = in_mem_store(Member)
        setup_members(member_store)
        persisty_context.register_store(member_store)

    def test_read_missing(self):
        empty = BandEntity()
        assert empty.members is None

    def test_destroy_cascade(self):
        class CascadingBandEntity(EntityABC, Band):
            members: List[MEMBER_ENTITY_CLASS] = HasMany(foreign_key_attr='band_id',
                                                         inverse_attr='_band',
                                                         on_destroy=OnDestroy.CASCADE)
        self._do_destroy(CascadingBandEntity)
        members = list(get_persisty_context().get_store(Member).read_all(iter(BEATLES_MEMBER_IDS), error_on_missing=False))
        assert members == [None, None, None, None]

    @staticmethod
    def _do_destroy(entity):
        beatles = entity.read('beatles')
        assert {m.id for m in beatles.members} == BEATLES_MEMBER_IDS
        beatles.destroy()

    def test_destroy_nullify(self):
        class NullifyingBandEntity(EntityABC, Band):
            members: List[MEMBER_ENTITY_CLASS] = HasMany(foreign_key_attr='band_id',
                                                         inverse_attr='_band',
                                                         on_destroy=OnDestroy.NULLIFY)
        self._do_destroy(NullifyingBandEntity)
        for m in get_persisty_context().get_store(Member).read_all(iter(BEATLES_MEMBER_IDS)):
            assert m.band_id is None

    def test_destroy_invalid(self):
        with self.assertRaises(PersistyError):
            class NullifyingBandEntity(EntityABC, Band):
                members: Page[MEMBER_ENTITY_CLASS] = HasMany(foreign_key_attr='band_id',
                                                             inverse_attr='_band',
                                                             on_destroy=OnDestroy.NULLIFY)

            self._do_destroy(NullifyingBandEntity)