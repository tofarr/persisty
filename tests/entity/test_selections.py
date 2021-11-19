from unittest import TestCase

from persisty.entity.selections import Selections, from_selection_set_list


class TestSelections(TestCase):

    def test_from_list(self):
        selection_set_list = ['foo/bar/zap', 'foo/bar/bang']
        selections = from_selection_set_list(selection_set_list)
        expected = Selections(foo=Selections(
            bar=Selections(
                zap=Selections(),
                bang=Selections()
            )
        ))
        assert expected == selections

    def test_has_sub_selections(self):
        selections = from_selection_set_list(['foo/bar/zap', 'foo/bar/bang'])
        assert selections.has_sub_selections()
        assert not Selections().has_sub_selections()
