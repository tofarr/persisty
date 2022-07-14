from dataclasses import dataclass, field
from typing import Optional
from unittest import TestCase

from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.attr.has_many_attr import HasManyAttr
from persisty.entity.entity_abc import EntityABC
from persisty.entity.selections import from_selection_set_list
from persisty.errors import PersistyError
from persisty.storage.in_mem.in_mem_storage import in_mem_storage, InMemStorage
from persisty.storage.storage_context_abc import get_default_storage_context
from persisty.storage.storage_meta import storage_meta_from_dataclass
from tests.fixtures.item_types import Band
from tests.fixtures.storage import setup_in_mem_storage
from tests.fixtures.storage_data import populate_data, BANDS
from tests.fixtures.entities import BandEntity, MemberEntity


class TestEntity(TestCase):

    def setUp(self):
        storage_context = get_default_storage_context()
        setup_in_mem_storage(storage_context)
        populate_data(storage_context)

    def test_invalid_entity(self):
        with self.assertRaises(RuntimeError):
            class InvalidBandEntity(EntityABC, Band):
                members = HasManyAttr()

            InvalidBandEntity()

    def test_read(self):
        band = BandEntity.read('beatles')
        expected = next(b for b in BANDS if b.id == 'beatles')
        assert band.to_item() == expected

    def test_read_missing(self):
        band = BandEntity.read('weird_al')
        assert band is None

    def test_create(self):
        band = BandEntity(title='Jefferson Airplane', year_formed=1965)
        band.create()
        assert band.id is not None
        assert band.is_save_required is False
        loaded = BandEntity.read(band.id)
        assert loaded == band

    def test_update(self):
        band = BandEntity.read('rolling_stones')
        band.title = 'The Blues Boys'
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

    def test_existing(self):
        band = BandEntity(title='Bon Jovi')
        assert not band.is_existing
        band.id = 'bon_jovi'
        assert not band.is_existing
        band.save()
        assert band.is_existing

    def test_load(self):
        band = BandEntity('beatles', 'Beatles')
        band.load()
        assert band == BandEntity.read('beatles')
        with self.assertRaises(PersistyError):
            BandEntity().load()
        with self.assertRaises(PersistyError):
            BandEntity('not_existing_key').load()

    def test_resolve_all(self):
        member = MemberEntity.read('john')
        member.resolve_all(None)
        assert getattr(member, '_band', None) is None
        member.resolve_all(from_selection_set_list(['band']))
        assert getattr(member, '_band') == BandEntity.read('beatles')

    def test_frozen(self):

        @dataclass(frozen=True)
        class Cube:
            id: Optional[str]
            length: float

        with self.assertRaises(ValueError):
            class CubeEntity(Cube, EntityABC[Cube]):
                __storage__: in_mem_storage(Cube)  # Defining explicitly rather than deferring to the context
            CubeEntity('from_tray', 3)

    def test_non_init_fields(self):
        """ Test a weird situation where we have a field that is not part of init """

        @dataclass
        class Cube:
            id: Optional[str]
            length: float = field(default=0, init=False)

        class CubeMarshaller(MarshallerABC[Cube]):

            def __init__(self):
                super().__init__(Cube)

            def load(self, item: ExternalItemType) -> Cube:
                cube_ = Cube(item['id'])
                cube_.length = item.get('length') or 0
                return cube_

            def dump(self, item: Cube) -> ExternalItemType:
                return dict(id=item.id, length=item.length)

        marshaller = CubeMarshaller()
        storage_meta = storage_meta_from_dataclass(Cube)

        class CubeEntity(Cube, EntityABC[Cube]):
            __storage__ = InMemStorage(storage_meta, marshaller)

        cube = CubeEntity('from_tray')
        assert cube.to_item() == Cube('from_tray')
        cube.length = 3
        cube.save()
        assert CubeEntity.read('from_tray').length == 3

        assert marshaller.dump(cube) == dict(id='from_tray', length=3)
        assert marshaller.load(dict(id='from_tray', length=3)) == cube.to_item()
