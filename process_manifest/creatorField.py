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
        ret += f"{creator['fullName']} "
        if creator.get('nationality', False) or creator.get('lifeDates', False):
            ret += f"({creator.get('nationality', '')}{self._comma_between(creator.get('nationality', False), creator.get('lifeDates', False))}{creator.get('lifeDates', '')})"

        return ret.strip()

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


def _sort_by_role_primary(e):
    if (e.get("role", "").lower() == "primary"):
        return 0

    return 1


# python -c 'from creatorField import *; test()'
def test():
    # import pprint
    # pp = pprint.PrettyPrinter(indent=4)
    data = [{'attribution': '', 'role': 'Primary', 'fullName': 'Marie Victoire Lemoine', 'nationality': 'French', 'lifeDates': '1754 - 1820', 'startDate': '1754', 'endDate': '1820', 'livingFlag': '0', 'lifeFlag': '1'}, {'attribution': 'Formerly attributed to', 'role': 'Primary', 'fullName': 'Elisabeth Louise Vigée-LeBrun', 'nationality': 'French', 'lifeDates': '1755 - 1842', 'startDate': '1755', 'endDate': '1842', 'livingFlag': '0', 'lifeFlag': '1'}]
    creator = creatorField(data)
    print(creator.to_iiif())
    
