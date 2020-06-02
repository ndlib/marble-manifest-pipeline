# import os


class FixCreatorsInJsonObject():
    def __init__(self, config: dict):
        self.config = config
        # parent_folder = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "/"

    def fix_creators(self, nd_json: dict) -> dict:
        """ recursively go through all of nd_json fixing creators using these rules:
            1.  If both unknown creators and known creators exist, remove unknown node
            2.  For collections, if Creators is unknown, remove node, regardless of existance of Contributors.
            3.  For manifests
                If Creators is unknown and Contributors exist, remove creators node.
                If neither Creators nor Contributors exist, add Creators unknown node.
            4.  Do nothing for Files level. """
        current_level = nd_json.get("level", "")
        if current_level == "collection":
            nd_json = self.fix_creators_for_collection(nd_json)
        elif current_level == "manifest":
            nd_json = self.fix_creators_for_manifest(nd_json)
        if "items" in nd_json:
            for item in nd_json["items"]:
                if item.get("level", "") in ["manifest", "collection"]:
                    item = self.fix_creators(item)
        return nd_json

    def fix_creators_for_collection(self, nd_json: dict) -> dict:
        """ If an unknown node exists in creators, remove the node, regardless of the existance of contributors """
        if self.unknown_creators_exist(nd_json):
            nd_json = self.remove_unknown_creators(nd_json)
        return nd_json

    def fix_creators_for_manifest(self, nd_json: dict) -> dict:
        """ If contributors exist and unknown creators exist, remove unknown creators node.
            If neither Creators nor Contributors exist, add Creators unknown node. """
        nd_json = self.remove_unknown_creators_if_known_creators_exist(nd_json)
        unknown_creators_exist_flag = self.unknown_creators_exist(nd_json)
        if self.contributors_exist(nd_json):
            if unknown_creators_exist_flag:
                nd_json = self.remove_unknown_creators(nd_json)
        elif not unknown_creators_exist_flag:
            nd_json = self.add_unknown_creators(nd_json)
        return nd_json

    def remove_unknown_creators_if_known_creators_exist(self, nd_json: dict) -> dict:
        """ If we have both know and unknown creators, remove unknown creators """
        if self.known_creators_exist(nd_json) and self.unknown_creators_exist(nd_json):
            nd_json = self.remove_unknown_creators(nd_json)
        return nd_json

    def contributors_exist(self, nd_json: dict) -> bool:
        """ Return boolean defining if contributors node exists """
        results = False
        if "contributors" in nd_json:
            if len(nd_json["contributors"]) > 0:
                results = True
        return results

    def unknown_creators_exist(self, nd_json: dict) -> bool:
        """ Return boolean defining if unknown creators node exists """
        results = False
        if "creators" in nd_json:
            for creator in nd_json["creators"]:
                if creator.get("fullName", "") == "unknown" or creator.get("display", "") == "unknown":
                    results = True
        return results

    def known_creators_exist(self, nd_json: dict) -> bool:
        """ Return boolean defining if known creators node exists """
        results = False
        if "creators" in nd_json:
            for creator in nd_json["creators"]:
                if creator.get("fullName", "") != "unknown" and creator.get("display", "") != "unknown":
                    results = True
        return results

    def remove_unknown_creators(self, nd_json: dict) -> dict:
        """ Remove unknown creators node """
        if "creators" in nd_json:
            i = len(nd_json["creators"])
            while i > 0:
                creator = nd_json["creators"][i - 1]
                if creator.get("fullName", "") == "unknown" or creator.get("display", "") == "unknown":
                    nd_json["creators"].pop(i - 1)
                i -= 1
        return nd_json

    def add_unknown_creators(self, nd_json: dict) -> dict:
        """ Add node for unknown creators """
        if "creators" not in nd_json:
            nd_json["creators"] = []
        unknown_node = {}
        unknown_node["fullName"] = "unknown"
        unknown_node["display"] = unknown_node["fullName"]
        nd_json["creators"].append(unknown_node)
        return nd_json
