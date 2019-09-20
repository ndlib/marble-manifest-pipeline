import os


class iiifItem:
    def __init__(self, id, type):
        self.id = id
        self.type = type.lower()
        self.lang = 'en'

    def get_id(self):
        return self.id

    def get_type(self):
        return self.type

    def item(self):
        return {
            'id': self.get_id(),
            'type': self.get_type()
        }

    def _lang_wrapper(self, line):
        return {self.lang: [line]}

    def label_wrapper(self, label):
        return {"label": self._lang_wrapper(label)}

    def value_wrapper(self, value):
        return {"value": self._lang_wrapper(value)}

    def filename_with_tif_extension(self, file):
        return os.path.splitext(file)[0] + '.tif'
