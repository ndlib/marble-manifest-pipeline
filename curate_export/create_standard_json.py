# create_standard_json.py
from translate_curate_json_node import TranslateCurateJsonNode
from pathlib import Path
from pipelineutilities.add_files_to_json_object import change_file_extensions_to_tif
from pipelineutilities.validate_json import validate_standard_json
from clean_up_standard_json import clean_up_standard_json


class CreateStandardJson():
    """ This performs all Curate API processing """
    def __init__(self, config):
        self.config = config
        self.sequences_within_parent = {}
        self.translate_curate_json_node_class = TranslateCurateJsonNode(config)

    def _get_ancestry_list(self, ancestry_array: list) -> list:
        """ This is always an array containing a single string separated by slash (/) """
        ancestry_string = ancestry_array[0]
        ancestry_string = ancestry_string.replace("und:", "")
        ancestry_list = ancestry_string.split("/")
        return ancestry_list

    def build_standard_json_tree(self, standard_json: dict, members: dict) -> dict:
        """ Use "members" nodes to create standard_json nodes, and append to standard_json tree.
            Note, because results of Curate "partOf" are not ordered, we may attempt to process children before their ancestors.
            Because of this, we need to attempt to attach children multiple times until we have no unprocessed members. """
        if standard_json:
            iterations_remaining = 5
            while iterations_remaining > 0:
                iterations_remaining -= 1  # prevent endless loop in case something is messed up coming out of Curate
                self._append_child_nodes(standard_json, members)
                report_unprocessed_members_flag = (iterations_remaining == 0)
                if self._count_unprocessed_members(members, report_unprocessed_members_flag) == 0:
                    break
            standard_json = clean_up_standard_json(standard_json, self.config)
        if not validate_standard_json(standard_json):
            standard_json = {}
        return standard_json

    def _append_child_nodes(self, standard_json: dict, members: list) -> dict:
        """ Append all child nodes for which parents are in the tree when a given "member" is attempted to be added. """
        for member in members:
            for _member_key, member_value in member.items():
                if not isinstance(member_value, bool) and not member_value.get("processed", False):
                    ancestry_list = self._get_ancestry_list(member_value.get("partOf", []))
                    parent_node = self._get_parent_node(standard_json, ancestry_list)
                    if parent_node:
                        child_json = self.translate_curate_json_node_class.build_json_from_curate_json(member_value, "root", {})
                        child_json = self._remove_unwanted_children(child_json)
                        child_json["collectionId"] = parent_node["collectionId"]
                        parent_id = parent_node["id"]
                        child_json["parentId"] = parent_id
                        child_json["sequence"] = self._accumulate_sequences_by_parent(parent_id)
                        if "items" not in parent_node:
                            parent_node["items"] = []
                        parent_node["items"].append(child_json)
                        member_value["processed"] = True
        return standard_json

    def _get_parent_node(self, json_object: dict, ancestry_list: list, iteration: int = 0) -> dict:
        """ Return immediate parent of leaf node.  list element 0 corresponds to a leaf node. """
        parent_node = {}
        max_depth_of_members_tree = 5
        local_ancestry_list = list(ancestry_list)
        if len(local_ancestry_list) > 1:
            iteration += 1
            parent_to_find = local_ancestry_list.pop(0)  # pop off the earliest ancestor
            if json_object["id"] == parent_to_find:
                parent_node = json_object
            elif "items" in json_object:
                for item in json_object["items"]:
                    if item.get("id", "") == parent_to_find:
                        parent_node = item
                        break
            if parent_node and len(local_ancestry_list) > 1 and iteration < max_depth_of_members_tree:  # early out if nested unreasonably deeply
                parent_node = self._get_parent_node(parent_node, local_ancestry_list, iteration)
        return parent_node

    def _count_unprocessed_members(self, members: dict, report_missing_members: bool = False) -> int:
        """ See if we have any "members" returned from Curate Json that have not been linked to standard "children" nodes """
        count_unprocessed = 0
        for member in members:
            for member_key, member_value in member.items():
                if not isinstance(member_value, bool) and not member_value.get("processed", False):
                    count_unprocessed += 1
                    if report_missing_members:
                        print("Member did not process: ", member_key, member_value.get("partOf", ""))
        return count_unprocessed

    def _accumulate_sequences_by_parent(self, my_parent_id: str) -> int:
        """ Save sequences already associated with parent to sequentially append to list """
        sequence_to_use = 1
        if my_parent_id in self.sequences_within_parent:
            sequence_to_use = self.sequences_within_parent[my_parent_id] + 1
        self.sequences_within_parent[my_parent_id] = sequence_to_use
        return sequence_to_use

    def _is_unwanted_child(self, child_json: dict) -> bool:
        """ We don't want to accumulate "file" records of type "jpg", "jpeg", (these should also have tiff equivalents included) or "xml" """
        if child_json.get('level') == 'file':
            file_name = child_json.get('title')
            file_extension = Path(file_name).suffix
            if file_extension in self.config.get('unwanted-file-extensions-from-curate', []):
                return True
        return False

    def _remove_unwanted_children(self, standard_json: dict) -> dict:
        """ Remove child file items we don't want """
        if 'items' in standard_json:
            standard_json['items'][:] = [item for item in standard_json['items'] if not self._is_unwanted_child(item)]
            for item in standard_json['items']:
                item = change_file_extensions_to_tif(item, self.config.get("file-extensions-to-protect-from-changing-to-tif", []))
        return standard_json
