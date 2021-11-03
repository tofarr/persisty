from unittest import TestCase

from persisty import get_persisty_context
from persisty.errors import PersistyError
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.obj_graph.entity_abc import EntityABC
from persisty.obj_graph.resolver.before_destroy import OnDestroy
from persisty.obj_graph.resolver.count import Count
from persisty.store.in_mem_store import in_mem_store
from tests.fixtures.data import setup_bands, setup_members
from tests.fixtures.entities import MemberEntity
from tests.fixtures.items import Band, Member

BEATLES_MEMBER_IDS = {'john', 'paul', 'george', 'ringo'}


class CountBandula(EntityABC, Band):
    num_members: int = Count(foreign_key_attr='band_id',
                             entity_type=MemberEntity)


class TestCount(TestCase):

    def setUp(self):
        persisty_context = get_persisty_context()
        band_store = in_mem_store(Band)
        setup_bands(band_store)
        persisty_context.register_store(band_store)
        member_store = in_mem_store(Member)
        setup_members(member_store)
        persisty_context.register_store(member_store)

    def test_count(self):
        beatles = CountBandula.read('beatles')
        assert beatles.num_members == 4

    def test_count_missing(self):
        empty = CountBandula()
        assert empty.num_members is None

    def test_destroy_no_action(self):
        self._do_destroy(CountBandula)
        assert MemberEntity.count(AttrFilter('band_id', AttrFilterOp.eq, 'beatles')) == 4

    def test_destroy_cascade(self):
        class CascadingCountBandula(EntityABC, Band):
            num_members: int = Count(foreign_key_attr='band_id',
                                     entity_type=MemberEntity,
                                     on_destroy=OnDestroy.CASCADE)
        self._do_destroy(CascadingCountBandula)
        members = list(get_persisty_context().get_store(Member).read_all(iter(BEATLES_MEMBER_IDS),
                                                                         error_on_missing=False))
        assert members == [None, None, None, None]

    def test_destroy_nullify(self):
        class NullifyingCountBandula(EntityABC, Band):
            num_members: int = Count(foreign_key_attr='band_id',
                                     entity_type=MemberEntity,
                                     on_destroy=OnDestroy.NULLIFY)

        self._do_destroy(NullifyingCountBandula)
        for m in get_persisty_context().get_store(Member).read_all(iter(BEATLES_MEMBER_IDS)):
            assert m.band_id is None

    @staticmethod
    def _do_destroy(entity):
        beatles = entity.read('beatles')
        assert beatles.num_members == 4
        beatles.destroy()

    def test_set(self):
        beatles = CountBandula.read('beatles')
        with self.assertRaises(PersistyError):
            beatles.num_members = 5
