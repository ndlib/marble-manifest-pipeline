# clean_up_content.py
from datetime import datetime
from add_image_records_as_child_items import AddImageRecordsAsChildItems
from dependencies.pipelineutilities.creatorField import creatorField  # noqa: E402
from pipelineutilities.csv_collection import _add_additional_paths
from html.parser import HTMLParser


class CleanUpContent():
    """ This class accepts a JSON object representing one item of museum content and massages that to fit our needs for processing. """
    def __init__(self, config: dict, image_files: dict):
        self.config = config
        self.image_files = image_files
        # self.cleaned_up_content = CleanUpContent._clean_up_content(object, image_files)

    def clean_up_content(self, object: dict) -> dict:
        """ This calls all other modules locally """
        object = self._define_worktype(object)
        object = self._fix_modified_date(object)
        object = self._fix_creators(object)
        object = self._remove_bad_subjects(object)
        object = self._add_missing_required_fields(object)
        object = self._clean_up_creation_place(object)
        object = self._clean_up_object_special_characters(object)
        if object.get("level", "manifest") != "file" and "digitalAssets" in object:
            add_image_records_as_child_items_class = AddImageRecordsAsChildItems(self.image_files)
            object = add_image_records_as_child_items_class.add_images_as_children(object)
        if "children" in object and len(object["children"]) == 0:
            del object["children"]
        _add_additional_paths(object, self.config)
        if "items" in object:
            for item in object["items"]:
                self.clean_up_content(item)
        return object

    def _clean_up_object_special_characters(self, object: dict) -> dict:
        field_names_to_clean = ["title", "copyrightStatement", "description"]
        for name in field_names_to_clean:
            if name in object:
                object[name] = self._replace_special_characters(object[name])
        return object

    def _clean_up_creation_place(self, object: dict) -> dict:
        if "creationPlace" in object:
            if "county" in object["creationPlace"]:
                object["creationPlace"]["county"] = self._replace_special_characters(object["creationPlace"]["county"])
        return object

    def _replace_special_characters(self, field_string: str) -> str:
        field_string = field_string.replace("%20", " ")
        h = HTMLParser()
        field_string = h.unescape(field_string)
        return field_string

    def _fix_modified_date(self, object: dict) -> dict:
        """ force iso format for  modifedDate """
        if 'modifiedDate' in object:
            try:
                object['modifiedDate'] = datetime.strptime(object['modifiedDate'], '%m/%d/%Y %H:%M:%S').isoformat() + 'Z'
            except ValueError:
                pass
        return object

    def _fix_creators(self, object: dict) -> dict:
        """ Add logic to create a display node for each creator that differs based upon person or corporate contributor. """
        if object.get("level", "") == "collection" or object.get("level", "") == "manifest":
            if "creators" not in object:
                object["creators"] = []
            creators = object["creators"]
            if len(creators) == 0:
                creators = [{"fullName": "unknown"}]
            for creator in creators:
                creator["fullName"] = self._replace_special_characters(creator["fullName"])
            creator_field_class = creatorField(creators)
            object["creators"] = creator_field_class.add_displays()
        return object

    def _remove_bad_subjects(self, object: dict) -> dict:
        """ Remove any subjects with an authority of "none" (i.e. our own defined subjects)
            Retain only those with a legitimate authority. """
        if "subjects" in object:
            i = len(object["subjects"])
            while i > 0:
                if object["subjects"][i - 1].get("authority", "") == "none":
                    del object["subjects"][i - 1]
                i -= 1
        return object

    def _define_worktype(self, object: dict) -> dict:
        """ Overwrite worktype if classification is Decorative Arts... """
        classifiction = object.get("classification", "")
        if classifiction == "Decorative Arts, Craft, and Design":
            object['workType'] = classifiction
        if "classification" in object:
            del object['classification']
        return object

    def _add_missing_required_fields(self, object: dict) -> dict:
        """If an object is a child, but the parent isn't web-enabled, we have no parentId and collectionId defined."""
        if "parentId" not in object:
            object["parentId"] = "root"
        if "collectionId" not in object:
            object["collectionId"] = object.get("id", "")
        return object
