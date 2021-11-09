from dataclasses import dataclass, FrozenInstanceError, field
from typing import Optional
from unittest import TestCase

from marshy.default_context import new_default_context
from marshy.marshaller.marshaller_abc import MarshallerABC
from marshy.types import ExternalItemType

from persisty.persisty_context import get_default_persisty_context, PersistyContext
from persisty.errors import PersistyError
from persisty.obj_graph.entity_abc import EntityABC
from persisty.obj_graph.entity_marshaller_factory import EntityMarshallerFactory
from persisty.obj_graph.resolver.has_many import HasMany
from persisty.obj_graph.selection_set import from_selection_set_list
from persisty.store.in_mem_store import in_mem_store
from tests.fixtures.data import setup_bands, setup_members, BANDS
from tests.fixtures.entities import BandEntity, MemberEntity
from tests.fixtures.items import Band, Member


class TestEntity(TestCase):

    def setUp(self):
        persisty_context = get_default_persisty_context()
        band_store = in_mem_store(Band)
        setup_bands(band_store)
        persisty_context.register_store(band_store)
        member_store = in_mem_store(Member)
        setup_members(member_store)
        persisty_context.register_store(member_store)

    def test_invalid_entity(self):
        with self.assertRaises(RuntimeError):
            class InvalidBandEntity(EntityABC, Band):
                members = HasMany(foreign_key_attr='band_id')

            InvalidBandEntity()

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

    def test_existing(self):
        band = BandEntity(band_name='Bon Jovi')
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

        persisty_context = PersistyContext()
        persisty_context.register_store(in_mem_store(Cube))

        class CubeEntity(EntityABC[Cube], Cube):
            __persisty_context__ = persisty_context

        cube = CubeEntity('from_tray', 3)
        assert cube == Cube('from_tray', 3)
        cube.save()
        with self.assertRaises(FrozenInstanceError):
            setattr(cube, 'length', 4)

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
                return {**item.__dict__}

        marshaller_context = new_default_context()
        marshaller_context.register_marshaller(CubeMarshaller())
        marshaller_context.register_factory(EntityMarshallerFactory(200))

        persisty_context = PersistyContext()
        persisty_context.register_store(in_mem_store(Cube, marshaller_context=marshaller_context))

        class CubeEntity(EntityABC[Cube], Cube):
            __persisty_context__ = persisty_context

        cube = CubeEntity('from_tray')
        assert cube == Cube('from_tray')
        cube.length = 3
        cube.save()
        assert CubeEntity.read('from_tray').length == 3

        assert marshaller_context.dump(cube) == dict(id='from_tray', length=3)
        assert marshaller_context.load(CubeEntity, dict(id='from_tray', length=3)) == cube
