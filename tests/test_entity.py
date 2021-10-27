from unittest import TestCase

from persisty import get_persisty_context
from persisty.store.in_mem.in_mem_store import mem_store
from tests.fixtures.data import setup_bands, setup_members, BANDS
from tests.fixtures.entities import BandEntity, MemberEntity
from tests.fixtures.items import Band, BandFilter, Member, MemberFilter


class TestEntity(TestCase):

    def setUp(self):
        persisty_context = get_persisty_context()
        band_store = mem_store(Band, BandFilter)
        setup_bands(band_store)
        persisty_context.register_store(band_store)
        member_store = mem_store(Member, MemberFilter)
        setup_members(member_store)
        persisty_context.register_store(member_store)

    def test_read(self):
        band = BandEntity.read('beatles')
        expected = next(b for b in BANDS if b.id == 'beatles')
        assert band.to_item() == expected

    def test_read_missing(self):
        band = BandEntity.read('weird_al')
        assert band is None

    def test_create(self):
        band = BandEntity(band_name='Jefferson Airplane', year_formed=1965)
        band.create()
        assert band.id is not None
        assert band.is_save_required is False
        loaded = BandEntity.read(band.id)
        assert loaded == band

    def test_update(self):
        band = BandEntity.read('rolling_stones')
        band.band_name = 'The Blues Boys'
        assert band.is_save_required
        band.save()
        loaded = BandEntity.read(band.id)
        assert loaded == band

    def test_destroy(self):
        band = BandEntity.read('rolling_stones')
        band.destroy()
        assert BandEntity.read('rolling_stones') is None

    def test_belongs_to(self):
        member = MemberEntity.read('john')
        band = BandEntity.read('beatles')
        assert member.band == band

    def test_has_many(self):
        band = BandEntity.read('beatles')
        member_ids = {m.id for m in band.members}
        assert member_ids == {'john', 'paul', 'george', 'ringo'}
