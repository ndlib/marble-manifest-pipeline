# add_image_records_as_child_items
from pipelineutilities.add_files_to_json_object import change_file_extensions_to_tif


class AddImageRecordsAsChildItems():
    def __init__(self, image_files, config: dict):
        self.image_files = image_files
        self.config = config

    def add_images_as_children(self, object: dict) -> dict:
        """ This will add each image found on disk as a child item, and will delete the digitalAssets node """
        if "items" not in object:
            object["items"] = []
        for digital_asset in object.get("digitalAssets", {}):
            child_item = {}
            image_item = self._create_item_record_for_image(digital_asset)
            if image_item:  # we specifically exclude files we couldn't find on Google Drive
                from_parent_item = self._add_child_content_from_parent(object)
                child_item.update(image_item)
                child_item.update(from_parent_item)
                object["items"].append(child_item)
        del object["digitalAssets"]
        return object

    def _create_item_record_for_image(self, digital_asset: dict) -> dict:
        """ Create one item record with information from one image """
        image_item = {}
        file_name = digital_asset.get("fileDescription", "")
        if file_name in self.image_files:
            image_item['id'] = file_name  # note: this will be updated later to be treePath + fileName
            image_item['level'] = 'file'
            image_item['sequence'] = int(digital_asset.get("sequence", 0))
            image_item['title'] = file_name
            image_item['description'] = file_name
            image_item['thumbnail'] = digital_asset.get("thumbnail", False)
            google_image_info = self.image_files[file_name]
            image_item['md5Checksum'] = google_image_info['md5Checksum']
            image_item['sourceUri'] = 'https://drive.google.com/a/nd.edu/file/d/' + google_image_info['id'] + '/view'
            # note: filePath will be added later to be the treePath + fileName
            image_item['fileId'] = google_image_info['id']
            image_item['modifiedDate'] = google_image_info['modifiedTime']
            image_item['mimeType'] = google_image_info['mimeType']
            image_item['size'] = int(google_image_info['size'])
            image_item['sourceType'] = 'Museum'
            image_item = change_file_extensions_to_tif(image_item, self.config.get("file-extensions-to-protect-from-changing-to-tif", []))
        return image_item

    def _add_child_content_from_parent(self, object: dict) -> dict:
        """ Get metadata from parent that should be inherited for this child """
        from_parent_item = {}
        from_parent_item['collectionId'] = object['collectionId']
        from_parent_item['parentId'] = object['id']
        from_parent_item['sourceSystem'] = object['sourceSystem']
        from_parent_item['repository'] = object['repository']
        if 'copyrightStatus' in object:
            from_parent_item['copyrightStatus'] = object['copyrightStatus']
        if 'copyrightStatemtnt' in object:
            from_parent_item['copyrightStatement'] = object['copyrightStatement']
        from_parent_item['objectFileGroupId'] = object['id']  # Once imageGroupId is adopted, we will remove this
        from_parent_item['imageGroupId'] = object['id']
        return from_parent_item
