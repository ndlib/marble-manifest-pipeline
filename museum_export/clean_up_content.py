# clean_up_content.py
from datetime import datetime
from dependencies.pipelineutilities.creatorField import creatorField  # noqa: E402


class CleanUpContent():
    """ This class accepts a JSON object representing one item of museum content and massages that to fit our needs for processing. """
    def __init__(self, object):
        self.cleaned_up_content = CleanUpContent._clean_up_content(object)

    @staticmethod
    def _clean_up_content(object):
        """ This calls all other modules locally """
        object = CleanUpContent._define_worktype(object)
        object = CleanUpContent._fix_modified_date(object)
        object = CleanUpContent._fix_creators(object)
        object = CleanUpContent._remove_bad_subjects(object)
        object = CleanUpContent._add_missing_required_fields(object)
        return object

    @staticmethod
    def _fix_modified_date(object):
        """ force iso format for  modifedDate """
        if 'modifiedDate' in object:
            try:
                object['modifiedDate'] = datetime.strptime(object['modifiedDate'], '%m/%d/%Y %H:%M:%S').isoformat() + 'Z'
            except ValueError:
                pass
        return object

    @staticmethod
    def _fix_creators(object):
        """ Add logic to create a display node for each creator that differs based upon person or corporate contributor. """
        if "creators" in object:
            creator_field_class = creatorField(object["creators"])
            object["creators"] = creator_field_class.add_displays()
        return object

    @staticmethod
    def _remove_bad_subjects(object):
        """ Remove any subjectes with an authority of "none" (i.e. our own defined subjects)
            Retain only those with a legitimate authority. """
        if "subjects" in object:
            i = len(object["subjects"])
            while i > 0:
                if object["subjects"][i - 1].get("authority", "") == "none":
                    del object["subjects"][i - 1]
                i -= 1
        return object

    @staticmethod
    def _define_worktype(object):
        """ Overwrite worktype if classification is Decorative Arts... """
        classifiction = object.get("classification", "")
        if classifiction == "Decorative Arts, Craft, and Design":
            object['workType'] = classifiction
        if "classification" in object:
            del object['classification']
        return object

    @staticmethod
    def _add_missing_required_fields(object):
        """If an object is a child, but the parent isn't web-enabled, we have no parentId and collectionId defined."""
        if "parentId" not in object:
            object["parentId"] = "root"
        if "collectionId" not in object:
            object["collectionId"] = object.get("id", "")
        return object
