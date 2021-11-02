from unittest import TestCase

from persisty import get_persisty_context
from persisty.obj_graph.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph.selection_set import from_selection_set_list
from persisty.store.in_mem_store import in_mem_store
from tests.fixtures.data import setup_bands, setup_members
from tests.fixtures.entities import BandEntity, MemberEntity
from tests.fixtures.items import Band, Member


class TestBelongsTo(TestCase):

    def setUp(self):
        persisty_context = get_persisty_context()
        band_store = in_mem_store(Band)
        setup_bands(band_store)
        persisty_context.register_store(band_store)
        member_store = in_mem_store(Member)
        setup_members(member_store)
        persisty_context.register_store(member_store)

    def test_read_missing(self):
        empty = MemberEntity.read('john')
        empty.band_id = None
        assert empty.band is None

    def test_read_multi(self):
        deferred_resolutions = DeferredResolutionSet()
        band = BandEntity.read('beatles', from_selection_set_list(['members/band']), deferred_resolutions)
        deferred_resolutions.resolve()
        get_persisty_context().get_store(Band).destroy('beatles')
        member = MemberEntity.read('john', from_selection_set_list(['band']), deferred_resolutions)
        assert member.band == band

    def test_set(self):
        john = MemberEntity.read('john')
        john.band = BandEntity('bon_jovi', 'Bon Jovi')
        assert john.band_id == 'bon_jovi'
        assert john.is_save_required

    def test_set_none(self):
        john = MemberEntity.read('john')
        john.band = None
        assert john.band_id is None
        assert john.is_save_required

    def test_create_connected(self):
        grace = MemberEntity('grace', 'Grace Slick', None, '1939-10-30')
        grace.band = BandEntity('jefferson', 'Jefferson Airplane')
        grace.save()
        assert not grace.is_save_required
        assert not grace.band.is_save_required
        assert grace.band == BandEntity.read('jefferson')
