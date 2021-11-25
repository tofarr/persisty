from unittest import TestCase

from marshy import get_default_context

from old.persisty.persisty_context import get_default_persisty_context
from persisty.obj_graph import from_selection_set_list
from old.persisty.storage.in_mem_storage import in_mem_storage
from tests.old.fixtures.data import setup_bands, setup_members
from tests.fixtures.entities import MemberEntity
from tests.old.fixtures.items import Band, Member


class TestEntity(TestCase):

    def setUp(self):
        persisty_context = get_default_persisty_context()
        band_storage = in_mem_storage(Band)
        setup_bands(band_storage)
        persisty_context.register_storage(band_storage)
        member_storage = in_mem_storage(Member)
        setup_members(member_storage)
        persisty_context.register_storage(member_storage)

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
                'title': 'The Beatles',
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
                'title': 'The Beatles',
                'year_formed': 1960
            }
        }
        loaded = get_default_context().load(MemberEntity, member)
        dumped = get_default_context().dump(loaded)
        assert dumped == member
