# create_standard_json.py
from translate_curate_json_node import TranslateCurateJsonNode


class CreateStandardJson():
    """ This performs all Curate API processing """
    def __init__(self, config, event):
        self.sequences_within_parent = {}
        self.translate_curate_json_node_class = TranslateCurateJsonNode(config, event)

    def _get_ancestry_list(self, ancestry_array):
        """ This is always an array containing a single string separated by slash (/) """
        ancestry_string = ancestry_array[0]
        ancestry_string = ancestry_string.replace("und:", "")
        ancestry_list = ancestry_string.split("/")
        return ancestry_list

    def build_standard_json_tree(self, standard_json, members):
        """ Use "members" nodes to create standard_json nodes, and append to standard_json tree.
            Note, because results of Curate "partOf" are not ordered, we may attempt to process children before their ancestors.
            Because of this, we need to attempt to attach children multiple times until we have no unprocessed members. """
        iterations_remaining = 5
        while iterations_remaining > 0:
            iterations_remaining -= 1  # prevent endless loop
            self._append_child_nodes(standard_json, members)
            report_unprocessed_members_flag = (iterations_remaining == 0)
            if self._count_unprocessed_members(members, report_unprocessed_members_flag) == 0:
                break
        return standard_json

    def _append_child_nodes(self, standard_json, members):
        """ Append all child nodes for which parents are in the tree when a given "member" is attempted to be added. """
        parent_id = standard_json["id"]
        for member in members:
            for member_key, member_value in member.items():
                if not member_value.get("processed", False):
                    ancestry_list = self._get_ancestry_list(member_value.get("partOf", []))
                    parent_node = self._get_parent_node(standard_json, ancestry_list)
                    if parent_node:
                        child_json = self.translate_curate_json_node_class.build_json_from_curate_json(member_value)
                        child_json["collectionId"] = parent_node["collectionId"]
                        parent_id = parent_node["id"]
                        child_json["parentId"] = parent_id
                        child_json["sequence"] = self._accumulate_sequences_by_parent(parent_id)
                        self._fix_child_file_info(child_json)
                        if "children" not in parent_node:
                            parent_node["children"] = []
                        parent_node["children"].append(child_json)
                        member_value["processed"] = True
        return standard_json

    def _get_parent_node(self, json_object, ancestry_list, iteration=0):
        """ Return immediate parent of leaf node.  list element 0 corresponds to a leaf node. """
        parent_node = {}
        max_depth_of_members_tree = 5
        local_ancestry_list = list(ancestry_list)
        if len(local_ancestry_list) > 1:
            iteration += 1
            parent_to_find = local_ancestry_list.pop(0)  # pop off the earliest ancestor
            if json_object["id"] == parent_to_find:
                parent_node = json_object
            elif "children" in json_object:
                for child in json_object["children"]:
                    if child.get("id", "") == parent_to_find:
                        parent_node = child
                        break
            if parent_node and len(local_ancestry_list) > 1 and iteration < max_depth_of_members_tree:  # early out if nested unreasonably deeply
                parent_node = self._get_parent_node(parent_node, local_ancestry_list, iteration)
        return parent_node

    def _fix_child_file_info(self, child_json):
        """ set collectionId, parentId, and thumbnail flag for each child file. """
        if "children" in child_json:
            for file_info in child_json["children"]:
                if file_info.get("workType") == "GenericFile":
                    file_info["collectionId"] = child_json["collectionId"]
                    file_info["parentId"] = child_json["id"]
                    file_info["thumbnail"] = (file_info.get("downloadUrl", "") == child_json.get("representativeImage", ""))
        return

    def _count_unprocessed_members(self, members, report_missing_members=False):
        """ See if we have any "members" returned from Curate Json that have not been linked to standard "children" nodes """
        count_unprocessed = 0
        for member in members:
            for member_key, member_value in member.items():
                if not member_value.get("processed", False):
                    count_unprocessed += 1
                    if report_missing_members:
                        print("Member did not process: ", member_key, member_value.get("partOf", ""))
        return count_unprocessed

    def _accumulate_sequences_by_parent(self, my_parent_id):
        """ Save sequences already associated with parent to sequentially append to list """
        sequence_to_use = 1
        if my_parent_id in self.sequences_within_parent:
            sequence_to_use = self.sequences_within_parent[my_parent_id] + 1
        self.sequences_within_parent[my_parent_id] = sequence_to_use
        return sequence_to_use
