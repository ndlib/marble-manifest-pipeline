class creatorField():

    def __init__(self, data):
        self.creator_data = data
        self.creator_data.sort(key=_sort_by_role_primary)

    def to_array(self):
        for creator in self.creator_data:
            yield self.make_string(creator)

    def make_string(self, creator):
        ret = ""
        if creator.get('attribution', False):
            ret += f"{creator['attribution']} "
        ret += f"{creator.get('fullName', '')} "
        if creator.get('nationality', False) or creator.get('lifeDates', False):
            ret += f"({creator.get('nationality', '')}{self._comma_between(creator.get('nationality', False), creator.get('lifeDates', False))}{creator.get('lifeDates', '')})"
        return ret.strip()

    def add_displays(self):
        """ Add display node to creator_data """
        for creator in self.creator_data:
            self._add_display(creator)
        return self.creator_data

    def _comma_between(self, nationality, lifeDates):
        if nationality and lifeDates:
            return ", "
        return ""

    def to_iiif(self):
        ret = []
        for creator in self.to_array():
            ret.append(creator)

        return ret

    def to_schema(self):
        ""

    def _make_lifeDate(self, creator):
        """ build lifeDate from startDate and endDate """
        start_date = creator.get("startDate", "")
        end_date = creator.get("endDate", "")
        results = ""
        if start_date:
            results = self._b_for_person_birthdate(creator) + self._active_for_corporate_creator(creator) + start_date
            if not creator.get("alive", True) or creator.get("endDate", ""):
                results += " - " + end_date
        if not results:
            results = creator.get("lifeDates", "")
        return results

    def _update_lifeDate(self, creator):
        creator["lifeDates"] = self._make_lifeDate(creator)

    def _add_display(self, creator):
        """ If node is not in creator, update lifeDate and add node with formatted string """
        if "display" not in creator:
            self._update_lifeDate(creator)
            creator["display"] = self.make_string(creator)

    def _b_for_person_birthdate(self, creator):
        if creator.get("human", False) and creator.get("alive", False) and creator.get("endDate", "") == "":
            return "b. "
        return ""

    def _active_for_corporate_creator(self, creator):
        """ Only return "active " if not human """
        if not creator.get("human", True):
            return "active "
        return ""


def _sort_by_role_primary(e):
    if (e.get("role", "").lower() == "primary"):
        return 0
    return 1


# python -c 'from creatorField import *; test()'
def test():
    # import pprint
    # pp = pprint.PrettyPrinter(indent=4)
    data = [
        {'attribution': '', 'role': 'Primary', 'fullName': 'Marie Victoire Lemoine', 'nationality': 'French', 'lifeDates': '1754 - 1820', 'startDate': '1754', 'endDate': '1820', 'alive': False, 'human': True},
        {'attribution': 'Formerly attributed to', 'role': 'Primary', 'fullName': 'Elisabeth Louise Vigée-LeBrun', 'nationality': 'French', 'lifeDates': '1755 - 1842', 'startDate': '1755', 'endDate': '1842', 'alive': False, 'human': True}  # noqa: E501
    ]
    creator = creatorField(data)
    print(creator.to_iiif())
