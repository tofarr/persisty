from unittest import TestCase

from persisty.item_comparator import MultiComparator, AttrComparator
from persisty.item_filter import AttrFilter, AttrFilterOp
from persisty.item_filter.and_filter import AndFilter
from persisty.item_filter.not_filter import NotFilter
from persisty.item_filter.or_filter import OrFilter
from persisty.search_filter import SearchFilter, search_filter_from_dataclass
from tests.fixtures.data import BANDS
from tests.fixtures.items import Band, BandFilter


class CustomSearchFilter:

    @staticmethod
    def to_search_filter(filter_type: Band) -> SearchFilter[Band]:
        assert filter_type is Band
        return SearchFilter(item_comparator=MultiComparator([
            AttrComparator('year_formed'),
            AttrComparator('band_name'),
        ]))


class TestEdit(TestCase):

    def test_to_search_filter_custom(self):
        search_filter = search_filter_from_dataclass(CustomSearchFilter(), Band)
        assert search_filter.item_comparator.key(BANDS[0]) == [BANDS[0].year_formed, BANDS[0].band_name]
        assert search_filter.item_filter is None

    def test_to_search_filter(self):
        search_filter = search_filter_from_dataclass(BandFilter(sort=['year_formed', 'band_name']), Band)
        assert search_filter.item_comparator.key(BANDS[0]) == [BANDS[0].year_formed, BANDS[0].band_name]
        assert search_filter.item_filter is None

    def test_to_search_filter_invalid(self):
        with self.assertRaises(ValueError):
            # noinspection PyTypeChecker
            search_filter_from_dataclass(BandFilter(sort=23), Band)

    def test_and_filter(self):
        a1 = AttrFilter('year_formed', AttrFilterOp.ne, 1900)
        a2 = AttrFilter('year_formed', AttrFilterOp.ne, 1901)
        b1 = AttrFilter('year_formed', AttrFilterOp.ne, 1902)
        b2 = AttrFilter('year_formed', AttrFilterOp.ne, 1903)
        a = AndFilter((a1, a2))
        b = AndFilter((b1, b2))
        assert a & b == AndFilter((a1, a2, b1, b2))
        assert a1 & b == AndFilter((a1, b1, b2))

    def test_endswith(self):
        assert AttrFilterOp.endswith.value('foobar', 'obar')
        assert not AttrFilterOp.endswith.value('foobar', 'oba')

    def test_oneof(self):
        assert AttrFilterOp.oneof.value('a', ['a', 'b', 'c'])
        assert not AttrFilterOp.oneof.value('d', ['a', 'b', 'c'])

    def test_not(self):
        filter_ = AttrFilter('year_formed', AttrFilterOp.eq, BANDS[0].year_formed)
        assert filter_.match(BANDS[0])
        filter_ = NotFilter(filter_)
        assert filter_.match(BANDS[0]) is False
        assert filter_.match(BANDS[1]) is True

    def test_or(self):
        filter_ = OrFilter((
            AttrFilter('year_formed', AttrFilterOp.eq, BANDS[0].year_formed),
            AttrFilter('year_formed', AttrFilterOp.eq, BANDS[1].year_formed)
        ))
        assert filter_.match(BANDS[0])
        assert filter_.match(BANDS[1])
        assert not filter_.match(BANDS[2])

    def test_attr_filter_none(self):
        filter_ = AttrFilter('year_formed', AttrFilterOp.gt, 1900)
        assert not filter_.match(Band('imaginary', 'Air Guitarists'))
        filter_ = AttrFilter('year_formed', AttrFilterOp.gt, None)
        assert not filter_.match(Band('imaginary', 'Air Guitarists', 2021))

