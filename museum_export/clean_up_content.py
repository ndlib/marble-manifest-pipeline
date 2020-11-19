# clean_up_content.py
from datetime import datetime, date
from add_image_records_as_child_items import AddImageRecordsAsChildItems
from dependencies.pipelineutilities.creatorField import creatorField  # noqa: E402
from pipelineutilities.csv_collection import _add_additional_paths
import html


class CleanUpContent():
    """ This class accepts a JSON standard_json representing one item of museum content and massages that to fit our needs for processing. """
    def __init__(self, config: dict, image_files: dict, api_version: int):
        self.config = config
        self.image_files = image_files
        self.api_version = api_version

    def clean_up_content(self, standard_json: dict) -> dict:
        """ This calls all other modules locally """
        if 'objectFileGroupId' not in standard_json:
            standard_json['objectFileGroupId'] = standard_json['id']
        standard_json = self._define_worktype(standard_json)
        standard_json = self._fix_modified_date(standard_json)
        standard_json = self._fix_creators(standard_json)
        standard_json = self._fix_subjects(standard_json)
        standard_json = self._remove_bad_subjects(standard_json)
        standard_json = self._add_missing_required_fields(standard_json)
        standard_json = self._clean_up_creation_place(standard_json)
        standard_json = self._clean_up_object_special_characters(standard_json)
        standard_json = self._define_digital_access(standard_json)
        standard_json = self._remove_unnecessary_relatedIds(standard_json, self._get_parent_child_id_list(standard_json))
        if standard_json.get("level", "manifest") != "file" and "digitalAssets" in standard_json:
            add_image_records_as_child_items_class = AddImageRecordsAsChildItems(self.image_files)
            standard_json = add_image_records_as_child_items_class.add_images_as_children(standard_json)
        if "children" in standard_json:
            del standard_json["children"]
        _add_additional_paths(standard_json, self.config)
        if "items" in standard_json:
            for item in standard_json["items"]:
                self.clean_up_content(item)
        return standard_json

    def _define_digital_access(self, standard_json: dict) -> dict:
        standard_json["digitalAccess"] = "Restricted"
        if "public" in standard_json.get("copyrightStatus", "").lower():
            standard_json["digitalAccess"] = "Regular"
        return standard_json

    def _clean_up_object_special_characters(self, standard_json: dict) -> dict:
        field_names_to_clean = ["title", "copyrightStatement", "description"]
        for name in field_names_to_clean:
            if name in standard_json:
                standard_json[name] = self._replace_special_characters(standard_json[name])
        return standard_json

    def _clean_up_creation_place(self, standard_json: dict) -> dict:
        if "creationPlace" in standard_json:
            if "county" in standard_json["creationPlace"]:
                standard_json["creationPlace"]["county"] = self._replace_special_characters(standard_json["creationPlace"]["county"])
        return standard_json

    def _replace_special_characters(self, field_string: str) -> str:
        field_string = field_string.replace("%20", "\n")
        field_string = html.unescape(field_string)
        return field_string

    def _fix_modified_date(self, standard_json: dict) -> dict:
        """ force iso format for  modifedDate """
        if 'modifiedDate' in standard_json:
            try:
                standard_json['modifiedDate'] = datetime.strptime(standard_json['modifiedDate'], '%m/%d/%Y %H:%M:%S').isoformat() + 'Z'
            except ValueError:
                pass
        return standard_json

    def _fix_creators(self, standard_json: dict) -> dict:
        """ Add logic to create a display node for each creator that differs based upon person or corporate contributor. """
        if standard_json.get("level", "") == "collection" or standard_json.get("level", "") == "manifest":
            if "creators" not in standard_json:
                standard_json["creators"] = []
            creators = standard_json["creators"]
            if len(creators) == 0:
                creators = [{"fullName": "unknown"}]
            for creator in creators:
                creator["fullName"] = self._replace_special_characters(creator["fullName"])
            creator_field_class = creatorField(creators)
            standard_json["creators"] = creator_field_class.add_displays()
        return standard_json

    def _fix_subjects(self, standard_json: dict) -> dict:
        """ Remove special html characters from subjects. """
        if "subjects" in standard_json:
            for subject in standard_json["subjects"]:
                if "term" in subject:
                    subject["term"] = self._replace_special_characters(subject["term"])
        return standard_json

    def _remove_bad_subjects(self, standard_json: dict) -> dict:
        """ Remove any subjects with an authority of "none" (i.e. our own defined subjects)
            Retain only those with a legitimate authority. """
        if "subjects" in standard_json:
            i = len(standard_json["subjects"])
            while i > 0:
                if standard_json["subjects"][i - 1].get("authority", "") == "none":
                    del standard_json["subjects"][i - 1]
                i -= 1
        return standard_json

    def _define_worktype(self, standard_json: dict) -> dict:
        """ Overwrite worktype if classification is Decorative Arts... """
        classifiction = standard_json.get("classification", "")
        if classifiction == "Decorative Arts, Craft, and Design":
            standard_json['workType'] = classifiction
        if "classification" in standard_json:
            del standard_json['classification']
        return standard_json

    def _add_missing_required_fields(self, standard_json: dict) -> dict:
        """If an standard_json is a child, but the parent isn't web-enabled, we have no parentId and collectionId defined."""
        if "parentId" not in standard_json:
            standard_json["parentId"] = "root"
        if "collectionId" not in standard_json:
            standard_json["collectionId"] = standard_json.get("id", "")
        standard_json["apiVersion"] = self.api_version
        standard_json["fileCreatedDate"] = str(date.today())
        return standard_json

    def _remove_unnecessary_relatedIds(self, standard_json: dict, parent_child_id_list: list) -> dict:
        """ In the dataset we get back from Web Kiosk, both parent-child relationships
            and also non-parent-sibling relationships are returned as relatedIds.
            Since the parent-child relationships are already captured as items,
            we need to remove those from relatedIds, so only non - parent - sibling relationships remain. """
        related_id_list = []
        if 'relatedIds' in standard_json:
            for related_id in standard_json['relatedIds']:
                r_id = related_id.get('id', '')
                if r_id and r_id not in parent_child_id_list:
                    related_id_list.append(related_id)
            del standard_json['relatedIds']
            if related_id_list:
                standard_json['relatedIds'] = related_id_list
        for item in standard_json.get('items', []):
            item = self._remove_unnecessary_relatedIds(item, parent_child_id_list)
        return standard_json

    def _get_parent_child_id_list(self, standard_json: dict) -> list:
        """ return a list of all ids defined in the parent/child structure """
        parent_child_id_list = []
        parent_child_id_list.append(standard_json['id'])
        for item in standard_json.get('items', []):
            if item.get('level', 'manifest') != 'file':
                parent_child_id_list.extend(self._get_parent_child_id_list(item))
        return parent_child_id_list
