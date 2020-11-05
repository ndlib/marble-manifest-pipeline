# test_clean_up_composite_json.py
""" test clean_up_composite_json """
import unittest
from clean_up_composite_json import CleanUpCompositeJson, _is_hierachy_searchable


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def test_1_find_parent_child_relationships(self):
        """ test_1 _find_parent_child_relationships """
        objects = {
            "abc": {"id": "abc", "children": [{"id": "abc.a"}, {"id": "abc.b"}]},
            "abc.a": {"id": "abc.a", "title": "thing a"},
            "abc.b": {"id": "abc.b", "title": "thing b"}
        }
        clean_up_content_class = CleanUpCompositeJson({})
        families_array = clean_up_content_class._find_parent_child_relationships(objects)
        expected_object = [{'parentId': 'abc', 'childId': 'abc.a', 'sequence': 0}, {'parentId': 'abc', 'childId': 'abc.b', 'sequence': 0}]
        self.assertEqual(families_array, expected_object)

    def test_2_remove_child_node_from_objects(self):
        """ test_2 _remove_child_node_from_objects """
        objects = {
            "abc": {"id": "abc", "children": [{"id": "abc.a"}, {"id": "abc.b"}]},
            "abc.a": {"id": "abc.a", "title": "thing a"},
            "abc.b": {"id": "abc.b", "title": "thing b"}
        }
        clean_up_content_class = CleanUpCompositeJson({})
        fixed_objects = clean_up_content_class._remove_child_node_from_objects(objects, "abc.b")
        self.assertTrue("abc.b" not in fixed_objects)

    def test_3_remove_child_node_from_parent_children_array(self):
        """ test_3 _remove_child_node_from_parent_children_array """
        objects = {
            "abc": {"id": "abc", "children": [{"id": "abc.a"}, {"id": "abc.b"}]},
            "abc.a": {"id": "abc.a", "title": "thing a"},
            "abc.b": {"id": "abc.b", "title": "thing b"}
        }
        clean_up_content_class = CleanUpCompositeJson({})
        fixed_objects = clean_up_content_class._remove_child_node_from_parent_children_array(objects, "abc", "abc.b")
        self.assertTrue("abc.b" not in fixed_objects["abc"].get("children", []))
        fixed_objects = clean_up_content_class._remove_child_node_from_parent_children_array(objects, "abc", "abc.a")
        self.assertTrue("abc.a" not in fixed_objects["abc"].get("children", []))

    def test_4_fix_parent_child_relationships(self):
        """ test_4 _fix_parent_child_relationships """
        objects = {
            "abc": {"id": "abc", "children": [{"id": "abc.a"}, {"id": "abc.b"}]},
            "abc.a": {"id": "abc.a", "title": "thing a"},
            "abc.b": {"id": "abc.b", "title": "thing b"}
        }
        clean_up_content_class = CleanUpCompositeJson({})
        fixed_objects = clean_up_content_class._fix_parent_child_relationships(objects)
        expected_object = {'abc': {'hierarchySearchable': False, 'id': 'abc', 'items': [{'id': 'abc.a', 'title': 'thing a', 'sequence': 0}, {'id': 'abc.b', 'title': 'thing b', 'sequence': 0}]}}
        self.assertEqual(fixed_objects, expected_object)

    def test_5_clean_up_composite_json(self):
        """ test_5 _clean_up_composite_json """
        composite_json = {
            "objects":
                {
                    "abc": {"id": "abc", "children": [{"id": "abc.a"}, {"id": "abc.b"}]},
                    "abc.a": {"id": "abc.a", "title": "thing a"},
                    "abc.b": {"id": "abc.b", "title": "thing b"}
                }
        }
        clean_up_content_class = CleanUpCompositeJson({})
        fixed_composite_json = clean_up_content_class._clean_up_composite_json(composite_json)
        expected_composite_json = {'objects': {'abc': {'hierarchySearchable': False, 'id': 'abc', 'items': [{'id': 'abc.a', 'title': 'thing a', 'sequence': 0}, {'id': 'abc.b', 'title': 'thing b', 'sequence': 0}]}}}
        self.assertEqual(fixed_composite_json, expected_composite_json)

    def test_6_cleaned_up_content(self):
        """ test_6_cleaned_up_content """
        composite_json = {
            "objects":
                {
                    "abc": {"id": "abc", "children": [{"id": "abc.a"}, {"id": "abc.b"}]},
                    "abc.a": {"id": "abc.a", "title": "thing a"},
                    "abc.b": {"id": "abc.b", "title": "thing b"}
                }
        }
        fixed_composite_json = CleanUpCompositeJson(composite_json).cleaned_up_content
        expected_composite_json = {'objects': {'abc': {'hierarchySearchable': False, 'id': 'abc', 'items': [{'id': 'abc.a', 'title': 'thing a', 'sequence': 0}, {'id': 'abc.b', 'title': 'thing b', 'sequence': 0}]}}}
        self.assertEqual(fixed_composite_json, expected_composite_json)

    def test_7_is_hierachy_searchable(self):
        """ test_7_is_hierachy_searchable """
        child_id = '1990.005.001.a'
        results = _is_hierachy_searchable(child_id)
        self.assertEqual(results, False)
        child_id = '2008.004.002'
        results = _is_hierachy_searchable(child_id)
        self.assertEqual(results, True)

    def test_8_cleaned_up_content_searchable(self):
        """ test_8_cleaned_up_content_searchable """
        composite_json = {
            "objects":
                {
                    "abc": {"id": "abc", "children": [{"id": "abc.1"}, {"id": "abc.2"}]},
                    "abc.1": {"id": "abc.1", "title": "thing a"},
                    "abc.2": {"id": "abc.2", "title": "thing b"}
                }
        }
        fixed_composite_json = CleanUpCompositeJson(composite_json).cleaned_up_content
        expected_composite_json = {'objects': {'abc': {'hierarchySearchable': True, 'id': 'abc', 'items': [{'id': 'abc.1', 'title': 'thing a', 'sequence': 0}, {'id': 'abc.2', 'title': 'thing b', 'sequence': 0}]}}}
        self.assertEqual(fixed_composite_json, expected_composite_json)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
