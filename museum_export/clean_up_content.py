# clean_up_content.py
from datetime import datetime


class CleanUpContent():
    """ This class accepts a JSON object representing one item of museum content and massages that to fit our needs for processing. """

    def clean_up_content(self, object):
        """ This calls all other modules locally """
        self._define_worktype(object)
        if 'modifiedDate' in object:
            object['modifiedDate'] = datetime.strptime(object['modifiedDate'], '%m/%d/%Y %H:%M:%S').isoformat() + 'Z'
        self._fix_creators(object)
        self._remove_bad_subjects(object)

    def _fix_creators(self, object):
        """ Add logic to create a display node for each creator that differs based upon person or corporate contributor. """
        if "creators" in object:
            for creator in object["creators"]:
                life_flag = creator.get("lifeFlag", "")
                if life_flag == "1":
                    creator_display = self._get_person_creator_display(creator)
                elif life_flag == "0":
                    creator_display = self._get_corporate_creator_display(creator)
                else:
                    creator_display = creator.get("fullName", "")
                creator["display"] = creator_display
                return
        return

    def _get_person_creator_display(self, creator):
        """ Format person creator """
        creator_display = creator.get("fullName", "")
        living_flag = creator.get("livingFlag", "0")
        start_date = creator.get("startDate", "")
        end_date = creator.get("endDate", "")
        nationality = creator.get("nationality", "")
        if nationality or start_date:
            creator_display += " ("
            if nationality:
                creator_display += nationality
                if start_date:
                    creator_display += ", "
            if start_date:
                if living_flag == "1":
                    creator_display += "b. " + start_date
                else:
                    creator_display += start_date + " - "
                    if end_date:
                        creator_display += end_date
            creator_display += ")"
        return creator_display

    def _get_corporate_creator_display(self, creator):
        """ Format corporate creator """
        creator_display = creator.get("fullName", "")
        start_date = creator.get("startDate", "")
        end_date = creator.get("endDate", "")
        nationality = creator.get("nationality", "")
        if nationality or start_date:
            creator_display += " ("
            if nationality:
                creator_display += nationality
                if start_date:
                    creator_display += ", "
            if start_date:
                creator_display += "active " + start_date + " - "
                if end_date:
                    creator_display += end_date
            creator_display += ")"
        return creator_display

    def _remove_bad_subjects(self, object):
        """ Remove any subjectes with an authority of "none" (i.e. our own defined subjects)
            Retain only those with a legitimate authority. """
        if "subjects" in object:
            i = len(object["subjects"])
            while i > 0:
                if object["subjects"][i - 1].get("authority", "") == "none":
                    del object["subjects"][i - 1]
                i -= 1

    def _define_worktype(self, object):
        """ Define worktype based on rules suggested. """
        classifiction = object.get("classification", "")
        if classifiction == "Decorative Arts, Craft, and Design":
            object['workType'] = classifiction
        del object['classification']
