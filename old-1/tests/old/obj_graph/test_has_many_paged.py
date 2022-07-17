from old.persisty.persisty_context import get_default_persisty_context
from persisty.errors import PersistyError
from persisty.obj_graph import EntityABC
from persisty.obj_graph import OnDestroy
from persisty.obj_graph import HasManyPaged
from old.persisty import Page
from tests.old.fixtures.data import BANDS
from tests.fixtures.entities import MEMBER_ENTITY_CLASS
from tests.old.fixtures.items import Band, Member
from tests.old.obj_graph.test_has_many import TestHasMany

BEATLES_MEMBER_IDS = {'john', 'paul', 'george', 'ringo'}


class BandEntityPaged(EntityABC, Band):
    members: Page[MEMBER_ENTITY_CLASS] = HasManyPaged(foreign_key_attr='band_id', limit=2)


class TestHasManyPaged(TestHasMany):

    def test_read_missing(self):
        empty = BandEntityPaged()
        assert empty.members is None

    def test_destroy_cascade(self):
        class CascadingBandEntity(EntityABC, Band):
            members: Page[MEMBER_ENTITY_CLASS] = HasManyPaged(foreign_key_attr='band_id', on_destroy=OnDestroy.CASCADE)
        self._do_destroy(CascadingBandEntity)
        members = list(get_default_persisty_context().get_storage(Member).read_all(iter(BEATLES_MEMBER_IDS),
                                                                                 error_on_missing=False))
        assert members == [None, None, None, None]

    @staticmethod
    def _do_destroy(entity):
        beatles = entity.read('beatles')
        assert {m.id for m in beatles.members.items} == BEATLES_MEMBER_IDS
        beatles.destroy()

    def test_destroy_nullify(self):
        class NullifyingBandEntity(EntityABC, Band):
            members: Page[MEMBER_ENTITY_CLASS] = HasManyPaged(foreign_key_attr='band_id', on_destroy=OnDestroy.NULLIFY)
        self._do_destroy(NullifyingBandEntity)
        for m in get_default_persisty_context().get_storage(Member).read_all(iter(BEATLES_MEMBER_IDS)):
            assert m.band_id is None

    def test_destroy_invalid(self):
        with self.assertRaises(RuntimeError):
            class NullifyingBandEntity(EntityABC, Band):
                members = HasManyPaged(foreign_key_attr='band_id', on_destroy=OnDestroy.NULLIFY)

            NullifyingBandEntity()

    def test_set(self):
        beatles = BandEntityPaged.read('beatles')
        with self.assertRaises(PersistyError):
            beatles.members = Page(BANDS)