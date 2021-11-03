from unittest import TestCase

from marshy import get_default_context

from persisty import get_persisty_context
from persisty.obj_graph.entity_marshaller_factory import EntityMarshallerFactory
from persisty.obj_graph.selection_set import from_selection_set_list
from persisty.store.in_mem_store import in_mem_store
from tests.fixtures.data import setup_bands, setup_members
from tests.fixtures.entities import MemberEntity
from tests.fixtures.items import Band, Member


class TestEntity(TestCase):

    def setUp(self):
        get_default_context().register_factory(EntityMarshallerFactory(200))

        persisty_context = get_persisty_context()
        band_store = in_mem_store(Band)
        setup_bands(band_store)
        persisty_context.register_store(band_store)
        member_store = in_mem_store(Member)
        setup_members(member_store)
        persisty_context.register_store(member_store)

    def test_dump(self):
        member = MemberEntity.read('john')
        member.resolve_all(from_selection_set_list(['band']))
        dumped = get_default_context().dump(member)
        expected = {
            'id': 'john',
            'member_name': 'John Lennon',
            'band_id': 'beatles',
            'date_of_birth': '1940-10-09',
            'band': {
                'id': 'beatles',
                'band_name': 'The Beatles',
                'year_formed': 1960
            }
        }
        assert dumped == expected

    def test_load(self):
        member = {
            'id': 'john',
            'member_name': 'John Lennon',
            'band_id': 'beatles',
            'date_of_birth': '1940-10-09',
            'band': {
                'id': 'beatles',
                'band_name': 'The Beatles',
                'year_formed': 1960
            }
        }
        loaded = get_default_context().load(MemberEntity, member)
        dumped = get_default_context().dump(loaded)
        assert dumped == member
