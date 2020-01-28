def nodes_exist_in_json_tree(json_tree, required_descendants):
    """ Returns boolean if all required nodes exists at any level in the passed json_tree """
    nodes_exist = True
    for required_descendant in required_descendants:
        if not node_exists_in_json_tree(json_tree, required_descendant):
            nodes_exist = False
            break
    return nodes_exist


def node_exists_in_dict(dict_passed, node_name):
    """ Test if one node exists anywhere in the dictionary passed """
    if node_name in dict_passed:
        return True
    else:
        for _key, value in dict_passed.items():
            result = node_exists_in_json_tree(value, node_name)
            if result:
                return result
    return False


def node_exists_in_json_tree(json_tree, node_name):
    """ Test if one node exists anywhere in the json_tree """
    if json_tree is None:
        return False
    if isinstance(json_tree, str):
        return False
    if isinstance(json_tree, dict):
        result = node_exists_in_dict(json_tree, node_name)
        if result:
            return result
    if isinstance(json_tree, list):
        for item in json_tree:
            result = node_exists_in_json_tree(item, node_name)
            if result:
                return result
    return False


# python -c 'from nodes_exist_in_json_tree import *; test()'
def test():
    """ test exection """
    json_tree = {}
    json_tree = {'abc': False, 'xyz': True}
    json_tree = {'def': [{'abc': False, 'xyz': True}]}
    json_tree = {'def': [{'xyz': True}, {'abc': False, 'xyz': False}, {'a': 'b'}]}
    json_tree = {'level': ['file'], 'scopecontent': ['ALS. In this short note to the unidentified "Mr. Gardner," Thomas Hardy indicates he will not be able to join a meeting of the "friends of parliamentary reform."'], 'unittitle': ['Letter. Thomas Hardy, n.p., to "Mr. Gardner," n.p.'], 'unitid': ['MSE/MD 3811-9'], 'physdesc': ['1 page on 1 sheet.'], 'unitdate': ['1821 April 2'], 'ark': ['https://archivesspace.library.nd.edu/ark:/56090/429098'], 'container': [{'parent': ['1'], 'type': ['Box'], 'label': ['Mixed Materials'], 'id': ['aspace_83f1317a50e9c57d7a3f4b6bbecf3886'], 'text': ['1']}, {'parent': ['aspace_83f1317a50e9c57d7a3f4b6bbecf3886'], 'type': ['Folder'], 'label': ['9'], 'id': ['aspace_69f492cfa1c7a692c0366bdae8bdbf74'], 'text': ['9']}], 'digitalAsset': [{'desc': ['Letter. Thomas Hardy, n.p., to Mr. Gardner; n.p., Page 1'], 'href': ['https://rarebooks.library.nd.edu/collections/ead_xml/images/MSE-MD_3811/MSE-MD_3811-09.a.150.jpg']}, {'desc': ['Letter. Thomas Hardy, n.p., to Mr. Gardner; n.p., Page 2'], 'href': ['https://rarebooks.library.nd.edu/collections/ead_xml/images/MSE-MD_3811/MSE-MD_3811-09.b.150.jpg']}]}  # noqa: #501
    required_descendants = ['digitalAsset']
    print(nodes_exist_in_json_tree(json_tree, required_descendants))
