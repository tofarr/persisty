from unittest import TestCase

from persisty.obj_graph import SelectionSet, from_selection_set_list


class TestEdit(TestCase):

    def test_from_list(self):
        selection_set_list = ['foo/bar/zap', 'foo/bar/bang']
        selections = from_selection_set_list(selection_set_list)
        expected = SelectionSet(foo=SelectionSet(
            bar=SelectionSet(
                zap=SelectionSet(),
                bang=SelectionSet()
            )
        ))
        assert expected == selections

    def test_has_sub_selections(self):
        selections = from_selection_set_list(['foo/bar/zap', 'foo/bar/bang'])
        assert selections.has_sub_selections()
        assert not SelectionSet().has_sub_selections()
