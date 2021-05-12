# import os


class FixCreatorsInJsonObject():
    def __init__(self, config: dict):
        self.config = config
        # parent_folder = os.path.dirname(os.path.dirname(os.path.realpath(__file__))) + "/"

    def fix_creators(self, standard_json: dict) -> dict:
        """ recursively go through all of standard_json fixing creators using these rules:
            1.  If both unknown creators and known creators exist, remove unknown node
            2.  For collections, if Creators is unknown, remove node, regardless of existance of Contributors.
            3.  For manifests
                If Creators is unknown and Contributors exist, remove creators node.
                If neither Creators nor Contributors exist, add Creators unknown node.
            4.  Do nothing for Files level.
            5.  Added 12/15/2020 - if the only creator is unknown, remove the creators node
                for all sourceSystems other than aleph. """
        current_level = standard_json.get("level", "")
        if current_level == "collection":
            standard_json = self.fix_creators_for_collection(standard_json)
        elif current_level == "manifest":
            standard_json = self.fix_creators_for_manifest(standard_json)
        if "items" in standard_json:
            for item in standard_json["items"]:
                if item.get("level", "") in ["manifest", "collection"]:
                    item = self.fix_creators(item)
        if standard_json.get("sourceSystem", "") != 'Aleph' and self.unknown_creators_exist(standard_json):
            standard_json.pop("creators", None)
        if standard_json.get("creators", []) == []:
            standard_json.pop("creators", None)
        return standard_json

    def fix_creators_for_collection(self, standard_json: dict) -> dict:
        """ If an unknown node exists in creators, remove the node, regardless of the existance of contributors """
        if self.unknown_creators_exist(standard_json):
            standard_json = self.remove_unknown_creators(standard_json)
        return standard_json

    def fix_creators_for_manifest(self, standard_json: dict) -> dict:
        """ If contributors exist and unknown creators exist, remove unknown creators node.
            If neither Creators nor Contributors exist, add Creators unknown node. """
        standard_json = self.remove_unknown_creators_if_known_creators_exist(standard_json)
        unknown_creators_exist_flag = self.unknown_creators_exist(standard_json)
        known_creators_exist_flag = self.known_creators_exist(standard_json)
        if self.contributors_exist(standard_json):
            if unknown_creators_exist_flag:
                standard_json = self.remove_unknown_creators(standard_json)
        elif known_creators_exist_flag:
            pass
        elif not unknown_creators_exist_flag:
            standard_json = self.add_unknown_creators(standard_json)
        return standard_json

    def remove_unknown_creators_if_known_creators_exist(self, standard_json: dict) -> dict:
        """ If we have both know and unknown creators, remove unknown creators """
        if self.known_creators_exist(standard_json) and self.unknown_creators_exist(standard_json):
            standard_json = self.remove_unknown_creators(standard_json)
        return standard_json

    def contributors_exist(self, standard_json: dict) -> bool:
        """ Return boolean defining if contributors node exists """
        results = False
        if "contributors" in standard_json:
            if len(standard_json["contributors"]) > 0:
                results = True
        return results

    def unknown_creators_exist(self, standard_json: dict) -> bool:
        """ Return boolean defining if unknown creators node exists """
        results = False
        if "creators" in standard_json:
            for creator in standard_json["creators"]:
                if creator.get("fullName", "") == "unknown" or creator.get("display", "") == "unknown":
                    results = True
        return results

    def known_creators_exist(self, standard_json: dict) -> bool:
        """ Return boolean defining if known creators node exists """
        results = False
        if "creators" in standard_json:
            for creator in standard_json["creators"]:
                if creator.get("fullName", "") != "unknown" and creator.get("display", "") != "unknown":
                    results = True
        return results

    def remove_unknown_creators(self, standard_json: dict) -> dict:
        """ Remove unknown creators node """
        if "creators" in standard_json:
            i = len(standard_json["creators"])
            while i > 0:
                creator = standard_json["creators"][i - 1]
                if creator.get("fullName", "") == "unknown" or creator.get("display", "") == "unknown":
                    standard_json["creators"].pop(i - 1)
                i -= 1
        return standard_json

    def add_unknown_creators(self, standard_json: dict) -> dict:
        """ Add node for unknown creators - only for Aleph sourceSystem """
        if standard_json.get("sourceSystem") != "Aleph":
            return standard_json
        if "creators" not in standard_json:
            standard_json["creators"] = []
        unknown_node = {}
        unknown_node["fullName"] = "unknown"
        unknown_node["display"] = unknown_node["fullName"]
        standard_json["creators"].append(unknown_node)
        return standard_json
