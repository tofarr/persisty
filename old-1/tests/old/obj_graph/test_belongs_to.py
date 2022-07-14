from unittest import TestCase

from old.persisty.persisty_context import get_default_persisty_context
from persisty.deferred.deferred_resolution_set import DeferredResolutionSet
from persisty.obj_graph import from_selection_set_list
from old.persisty.storage.in_mem_storage import in_mem_storage
from tests.old.fixtures.data import setup_bands, setup_members
from tests.fixtures.entities import BandEntity, MemberEntity
from tests.old.fixtures.items import Band, Member


class TestBelongsTo(TestCase):

    def setUp(self):
        persisty_context = get_default_persisty_context()
        band_storage = in_mem_storage(Band)
        setup_bands(band_storage)
        persisty_context.register_storage(band_storage)
        member_storage = in_mem_storage(Member)
        setup_members(member_storage)
        persisty_context.register_storage(member_storage)

    def test_read_missing(self):
        empty = MemberEntity.read('john')
        empty.band_id = None
        assert empty.band is None

    def test_read_multi(self):
        deferred_resolutions = DeferredResolutionSet()
        band = BandEntity.read('beatles', from_selection_set_list(['members/band']), deferred_resolutions)
        deferred_resolutions.resolve()
        get_default_persisty_context().get_storage(Band).destroy('beatles')
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
