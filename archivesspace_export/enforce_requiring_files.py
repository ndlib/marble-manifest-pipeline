from nodes_exist_in_json_tree import nodes_exist_in_json_tree


# Much more work exists to make this functional
# may not be needed, even if it is, do as a subsequent step.
def enforce_required_descendants(json_node: dict, required_descendants: str) -> dict:
    """ If required_descendants do not exist, set the json_node to None. """
    missing_required_descendants = False
    if required_descendants != "":
        missing_required_descendants = not nodes_exist_in_json_tree(json_node, required_descendants)
    if missing_required_descendants:
        json_node = None
    return json_node
