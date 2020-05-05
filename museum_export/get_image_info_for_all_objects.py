# get_image_info_for_all_objects
from find_google_images import FindGoogleImages


class GetImageInfoForAllObjects():
    """ This class searches through the objects hash to find all images,
        then calls a separate class to retrieve information from Google Drive
        for each of those images """
    def __init__(self, objects, google_credentials, drive_id):
        image_files_list = GetImageInfoForAllObjects._get_image_files_list(objects)
        self.image_file_info = GetImageInfoForAllObjects._find_images_for_all_objects(image_files_list, google_credentials, drive_id)

    @staticmethod
    def _find_images_for_all_objects(image_files_list: list, google_credentials: dict, drive_id: str) -> dict:
        find_google_images_class = FindGoogleImages(google_credentials, drive_id)
        image_files = find_google_images_class.get_image_file_info(image_files_list)
        if not image_files:
            image_files = {}
        return image_files

    @staticmethod
    def _get_image_files_list(objects: dict) -> list:
        image_files_list = []
        for _object_key, object_value in objects.items():
            if 'digitalAssets' in object_value:
                for digital_asset in object_value['digitalAssets']:
                    image_files_list.append(digital_asset['fileDescription'])
        return image_files_list
