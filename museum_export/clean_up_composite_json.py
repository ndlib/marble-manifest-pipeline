# clean_up_composite_json.py
# from datetime import datetime
# from dependencies.pipelineutilities.creatorField import creatorField  # noqa: E402


class CleanUpCompositeJson():
    """ This class accepts a JSON object representing one item of museum content and massages that to fit our needs for processing. """
    def __init__(self, composite_json):
        new_composite_json = CleanUpCompositeJson._clean_up_composite_json(composite_json)
        self.cleaned_up_content = new_composite_json

    @staticmethod
    def _clean_up_composite_json(composite_json):
        """ This calls all other modules locally """
        objects = composite_json.get("objects", {})
        objects = CleanUpCompositeJson._fix_parent_child_relationships(objects)
        composite_json["objects"] = objects
        return composite_json

    @staticmethod
    def _fix_parent_child_relationships(objects):
        families_array = CleanUpCompositeJson._find_parent_child_relationships(objects)
        for family in families_array:
            parent_id = family["parentId"]
            child_id = family["childId"]
            parent = objects[parent_id]
            if "items" not in parent:
                parent["items"] = []
            if child_id in objects:
                parent["items"].append(objects[child_id])
                CleanUpCompositeJson._remove_child_node_from_objects(objects, family["childId"])
                CleanUpCompositeJson._remove_child_node_from_parent_children_array(objects, parent_id, child_id)
        return objects

    @staticmethod
    def _find_parent_child_relationships(objects):
        families_array = []
        for object_key, object_value in objects.items():
            if "children" in object_value:
                for child in object_value["children"]:
                    node = {}
                    node["parentId"] = object_key
                    node["childId"] = child["id"]
                    families_array.append(node)
        return families_array

    @staticmethod
    def _remove_child_node_from_objects(objects, child_id):
        del objects[child_id]
        return objects

    @staticmethod
    def _remove_child_node_from_parent_children_array(objects, parent_id, child_id):
        if parent_id in objects:
            if "children" in objects[parent_id]:
                for index, child in enumerate(objects[parent_id]["children"]):
                    if child.get("id", "") == child_id:
                        objects[parent_id]["children"].pop(index)
                if len(objects[parent_id]["children"]) == 0:
                    del objects[parent_id]["children"]
        return objects
