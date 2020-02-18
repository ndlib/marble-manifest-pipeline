

class ndJson():
    def __init__(self, id, config, data):
        self.id = id
        self.config = config
        self.data = data

    def to_hash(self):
        return self._get_children(self.data)

    def _get_children(self, parent):
        output = self._fix_row(parent.object)
        output['children'] = []

        for child in parent.children():
            output['children'].append(self._get_children(child))

        return output

    def _fix_row(self, row):
        return row
