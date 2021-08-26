import _set_path  # noqa
import unittest
from pipelineutilities.search_files import id_from_url, url_can_be_harvested, file_should_be_skipped, is_tracked_file, is_directory, _convert_dict_to_camel_case, \
    is_media_file, augement_file_record


example_ids = {
    'https://rarebooks.nd.edu/digital/civil_war/letters/images/caley/5024-34.a.150.jpg': {'id': '5024-', 'group': '34', 'label': 'a'},
    'https://rarebooks.library.nd.edu/collections/ead_xml/images/MSN-EA_5026/MSN-EA_5026-20.a.150.jpg': {'id': 'MSN-EA_5026-', 'group': '20', 'label': 'a'},
    'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-016-c2.jpg': {'id': 'BPP_1001-', 'group': '016', 'label': 'c2'},
    'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-018.jpg': {'id': 'BPP_1001-', 'group': '018', 'label': ''},
    'https://rarebooks.nd.edu/digital/colonial_american/records/images/massachusetts/swansea/2718-05.a.150.jpg': {'id': '2718-', 'group': '05', 'label': 'a'},
    'https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8006-1-B/images/MSN-EA_8006-01-B-00a.jpg': {'id': 'MSN-EA_8006-', 'group': '01', 'label': 'B 00a'},
    'https://rarebooks.library.nd.edu/digital/bookreader/CodeLat_b04/images/CodeLat_b04-000a-front_cover.jpg': {'id': 'CodeLat_b04-', 'group': '', 'label': '000a front cover'},
    'https://rarebooks.nd.edu/digital/civil_war/papers_personal/images/edwards/1004-51.a_cvr.150.jpg': {'id': '1004-', 'group': '51', 'label': 'a cvr'},
    'https://rarebooks.nd.edu/digital/civil_war/letters/images/reeves/5012-23.bc.150.jpg': {'id': '5012-', 'group': '23', 'label': 'bc'},
    'https://rarebooks.nd.edu/digital/civil_war/diaries_journals/images/boardman/8000-00_cover_outside.150.jpg': {'id': '8000-', 'group': '00', 'label': 'cover outside'},
    'https://rarebooks.nd.edu/digital/civil_war/diaries_journals/images/arthur/8001-01.150.jpg': {'id': '8001-', 'group': '01', 'label': ''},
    'https://rarebooks.nd.edu/digital/civil_war/diaries_journals/images/cline/8007-000a.150.jpg': {'id': '8007-', 'group': '000', 'label': 'a'},
    'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-299-part1.jpg': {'id': 'BPP_1001-', 'group': '299', 'label': 'part1'},
}

# Note: I will leave these old tests here for now in case we need to again harvest from the RBSC bucket
temp_ids_example = {
    # 'https://rarebooks.library.nd.edu/collections/ead_xml/images/MSN-EA_5026/MSN-EA_5026-20.a.150.jpg': 'MSN-EA_5026-20',  # noqa: #501
    # 'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-016-c2.jpg': 'BPP_1001-016',  # noqa: #501
    # 'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-018.jpg': 'BPP_1001-018',  # noqa: #501
    # 'https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8006-1-B/images/MSN-EA_8006-01-B-00a.jpg': 'MSN-EA_8006-01',  # noqa: #501
    # 'https://rarebooks.library.nd.edu/digital/bookreader/CodeLat_b04/images/CodeLat_b04-000a-front_cover.jpg': 'CodeLat_b04',  # noqa: #501
    # 'https://rarebooks.library.nd.edu/digital/bookreader/El_Duende/images/El_Duende_5_000003.jpg': 'El_Duende',  # noqa: #501
    # 'https://rarebooks.library.nd.edu/digital/bookreader/MSS_CodLat_e05/images/MSS_CodLat_e05-084r.jpg': 'MSS_CodLat_e05',  # noqa: #501
    # 'https://rarebooks.library.nd.edu/digital/bookreader/images/CodLat_c03/MSS_CodLat_c3_098r.jpg': 'MSS_CodLat_c3',  # noqa: #501
    # 'https://rarebooks.library.nd.edu/digital/bookreader/Newberry-Case_MS_181/images/Newberry-Case_MS_181-999d.jpg': 'Newberry-Case_MS_181',  # noqa: #501
    # "https://rarebooks.nd.edu/digital/civil_war/letters/images/barker/5040-01.a.150.jpg": "5040-01",
    # "https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8011-1-B/images/MSN-EA_8011-01-B-000a.jpg": "MSN-EA_8011-01",  # noqa: #501
    # "https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8011-2-B/images/MSN-EA_8011-02-B-000a.jpg": "MSN-EA_8011-02",  # noqa: #501
    # "https://rarebooks.nd.edu/digital/civil_war/diaries_journals/images/moore/MSN-CW_8010-01.150.jpg": "MSN-CW_8010",  # noqa: #501
    # "https://rarebooks.nd.edu/digital/civil_war/letters/images/jordan/5000-01.a.150.jpg": "5000-01",
    # "https://rarebooks.nd.edu/digital/civil_war/letters/images/mckinney/5003-02.a.150.jpg": "5003-02",
    # "https://rarebooks.nd.edu/digital/colonial_american/records/images/massachusetts/bristol/2717-01.a.150.jpg": "2717-01",
    # "https://rarebooks.nd.edu/digital/civil_war/diaries_journals/images/arthur/8001-01.150.jpg": "8001",
    # "https://rarebooks.nd.edu/digital/civil_war/diaries_journals/images/arthur/8001-001.150.jpg": "8001",
    # "https://rarebooks.nd.edu/digital/civil_war/diaries_journals/images/arthur/8001-001a.150.jpg": "8001",
    # "https://rarebooks.nd.edu/digital/civil_war/diaries_journals/images/moore/MSN-CW_8010-00-cover2.150.jpg": "MSN-CW_8010",
    "https://marbleb-multimedia.library.nd.edu/Aleph/BOO_000297305/BOO_000297305_000001.tif": "BOO_000297305",  # noqa: #501
    "https://marbleb-multimedia.library.nd.edu/ArchivesSpace/MSSP_7000/MSSP_7000-02.tif": "MSSP_7000-02",  # noqa: #501
    "https://marbleb-multimedia.library.nd.edu/ArchivesSpace/MSE-REE_0006/MSE-REE_0006-010.a.jpg": "MSE-REE_0006-010",  # noqa: #501
    "https://marbleb-multimedia.library.nd.edu/ArchivesSpace/MSN-COL_9101-1-B/MSN-COL_9101-1-B_236.tif": "MSN-COL_9101-1",  # noqa: #501
    "https://marbleb-multimedia.library.nd.edu/ArchivesSpace/MSE-IR_1037-B/MSE-IR_1037B-0001.tif": "MSE-IR_1037B-0001",  # noqa: #501
    "https://marbleb-multimedia.library.nd.edu/ArchivesSpace/INQ_214a/INQ_214a-0001.tif": "INQ_214a",  # noqa: #501
    "https://marbleb-multimedia.library.nd.edu/ArchivesSpace/INQ_214a/INQ_034-0034.tif": "INQ_034",  # noqa: #501
    "https://marbleb-multimedia.library.nd.edu/ArchivesSpace/MSH-LAT_001/MSH-LAT_001-107-0001.tif": "MSH-LAT_001-107",  # noqa: #501
    "https://marble-multimedia.library.nd.edu/public-access/media/MyAudioCollection/Example.mp3": "MyAudioCollection",
}


valid_urls = [
    'https://rarebooks.library.nd.edu/path/to/file.jpg',
    'http://rarebooks.library.nd.edu/path/to/file.jpg',
]

invalid_urls = [
    'https://www.google.com/path/to/file.jpg',
    'https://someurl.com/path/file.jpg',
    'https://library.nd.edu/path/file.jpg',
]

skipped_files = [
    '._filename.jpg',
    'filename.100.jpg',
    'filename.072.jpg',
    'some/folder/with_resource.frk/file.jpg',
    '_file_to_skip.jpg',
    'example.someotherextension'
]

valid_files = [
    'filename.150.jpg',
    'filename.150.jpeg',
    'filename.tif',
    'filename.tiff',
    'filename.150.tif',
    'somepdf.something.pdf',
    'filename/ending/with_resource.frk',
    'movie.mp4',
    'audio.mp3',
    'audio2.wav'
]


# date example we may remediated saved for now.
# 'https://rarebooks.nd.edu/digital/civil_war/records_military/images/bloodgood/1864_07_10.c.150.jpg]': ['1864_07_10', 'c'],
class TestSearchFiles(unittest.TestCase):

    def test_id_from_url(self):
        for url in temp_ids_example:
            self.assertEqual(id_from_url(url), temp_ids_example[url])

    def test_url_can_be_harvested(self):
        for url in valid_urls:
            self.assertTrue(url_can_be_harvested(url))

        for url in invalid_urls:
            self.assertFalse(url_can_be_harvested(url))

    def test_file_should_be_skipped(self):
        for url in skipped_files:
            self.assertTrue(file_should_be_skipped(url))

        for url in valid_files:
            self.assertFalse(file_should_be_skipped(url))

    def test_is_jpg(self):
        tests = [
            'filename.jpg',
            'filename.jpeg',
            'filename.jpEg',
            'filename.JPG',
            'BOO_000297305_000001.tif',
            'file/is/still/ok.jpg'
        ]

        for test in tests:
            self.assertTrue(is_tracked_file(test))

    def test_is_tracked_file(self):
        failed_tests = ['file/is/resource.frk/not_ok.jpg']
        for test in failed_tests:
            self.assertFalse(is_tracked_file(test))

    def test_is_directory(self):
        true_tests = [
            'someththing/',
            'witha.ext/',
            '_underscore/',
            'multi/level/',
            'multi/level/with.ext/',
        ]

        for test in true_tests:
            self.assertTrue(is_directory(test))

    def test_is_not_a_directory(self):
        false_tests = [
            'someththing',
            '.withaleadingdot.ext/',
            '_underscore',
            'multi/level',
            'multi/level/with.ex',
            'multi/level/image.jpg',
            '',
            0
        ]

        for test in false_tests:
            self.assertFalse(is_directory(test), msg="Should Fail Test: %s" % test)

    def test_convert_dict_to_camel_case(self):
        tests = [
            {"This": "is", "a": " test"},
            {"ThisIs": "another", "SimpleTime": " test"},
        ]
        results = [
            {"this": "is", "a": " test"},
            {"thisIs": "another", "simpleTime": " test"},
        ]

        for i, test in enumerate(tests):
            result = _convert_dict_to_camel_case(test)
            self.assertEqual(result, results[i])

    def test_08_is_media_file(self):
        """ test_08_is_media_file """
        self.assertTrue(is_media_file(['.pdf', '.mp3', '.mp4'], "some/file.pdf"))
        self.assertFalse(is_media_file(['.pdf', '.mp3', '.mp4'], "some/file.tif"))
        self.assertTrue(is_media_file(['.pdf', '.mp3', '.mp4'], "https://some/file.pdf"))

    def test_09_augement_file_record(self):
        # expect wav file in folder served by CDN to return results with sourceType of Uri and sourceUri populated correctly
        obj = {'key': 'public-access/media/005065260/CROCKER_1_01_Universi.wav', 'lastModified': '2021-06-30T12:40:37+00:00', 'eTag': '9cbc7221e19bfbe2916e7ef0442ae2d9-2', 'size': 18120358, 'storageClass': 'STANDARD'}
        config = {
            "media-file-extensions": ['.pdf', '.mp3', '.mp4', '.wav'],
            "image-file-extensions": ['.tif', '.tiff', '.jpg', '.jpeg'],
            "media-server-base-url": "https://marbleb-multimedia.library.nd.edu",
            "marble-content-bucket": "libnd-smb-marble",
        }
        id = "005065260"
        url = 'https://marbleb-multimedia.library.nd.edu/public-access/media/005065260/CROCKER_1_01_Universi.wav'
        bucket_name = 'libnd-smb-marble'
        actual_results = augement_file_record(obj, id, url, config, bucket_name)
        expected_results = {
            'key': 'public-access/media/005065260/CROCKER_1_01_Universi.wav',
            'lastModified': '2021-06-30T12:40:37+00:00',
            'eTag': '9cbc7221e19bfbe2916e7ef0442ae2d9-2',
            'size': 18120358,
            'storageClass': 'STANDARD',
            'fileId': '005065260',
            'label': 'https://marbleb multimedia library nd edu/public access/media//CROCKER 1 01 Universi',
            'sourceType': 'Uri',
            'source': 'libnd-smb-marble',
            'path': 'media/005065260/CROCKER_1_01_Universi.wav',
            'sourceUri': 'https://marbleb-multimedia.library.nd.edu/media%2F005065260%2FCROCKER_1_01_Universi.wav',
            'sourceBucketName': 'libnd-smb-marble',
            'sourceFilePath': 'public-access/media/005065260/CROCKER_1_01_Universi.wav',
            'filePath': 'media/005065260/CROCKER_1_01_Universi.wav',
            'mediaGroupId': '005065260',
            'mimeType': 'audio/wav',
            'mediaResourceId': 'media%2F005065260%2FCROCKER_1_01_Universi.wav',
            'mediaServer': 'https://marbleb-multimedia.library.nd.edu',
            'typeOfData': 'Multimedia bucket'
        }
        self.assertEqual(actual_results, expected_results)

    def test_10_augement_file_record(self):
        # expect media file (other than pdf) in Aleph folder to return empty dict
        obj = {'key': 'Aleph/005065260/CROCKER_1_01_Universi.wav', 'lastModified': '2021-06-30T12:40:37+00:00', 'eTag': '9cbc7221e19bfbe2916e7ef0442ae2d9-2', 'size': 18120358, 'storageClass': 'STANDARD'}
        config = {
            "media-file-extensions": ['.pdf', '.mp3', '.mp4', '.wav'],
            "image-file-extensions": ['.tif', '.tiff', '.jpg', '.jpeg'],
            "media-server-base-url": "https://marbleb-multimedia.library.nd.edu",
            "marble-content-bucket": "libnd-smb-marble",
        }
        id = "005065260"
        url = 'https://marbleb-multimedia.library.nd.edu/public-access/media/005065260/CROCKER_1_01_Universi.wav'
        bucket_name = 'libnd-smb-marble'
        actual_results = augement_file_record(obj, id, url, config, bucket_name)
        expected_results = {}
        self.assertEqual(actual_results, expected_results)

    def test_11_augement_file_record(self):
        # expect pdf file in Aleph to return results with sourceType of S3 and sourceUri populated correctly
        obj = {'key': 'Aleph/005065260/CROCKER_1_01_Universi.pdf', 'lastModified': '2021-06-30T12:40:37+00:00', 'eTag': '9cbc7221e19bfbe2916e7ef0442ae2d9-2', 'size': 18120358, 'storageClass': 'STANDARD'}
        config = {
            "media-file-extensions": ['.pdf', '.mp3', '.mp4', '.wav'],
            "image-file-extensions": ['.tif', '.tiff', '.jpg', '.jpeg'],
            "media-server-base-url": "https://marbleb-multimedia.library.nd.edu",
            "marble-content-bucket": "libnd-smb-marble",
            "image-server-base-url": "https://image-iiif.library.nd.edu/iiif/2",
        }
        id = "005065260"
        url = 'https://marbleb-multimedia.library.nd.edu/Aleph/005065260/CROCKER_1_01_Universi.pdf'
        bucket_name = 'libnd-smb-marble'
        actual_results = augement_file_record(obj, id, url, config, bucket_name)
        expected_results = {
            'key': 'Aleph/005065260/CROCKER_1_01_Universi.pdf',
            'lastModified': '2021-06-30T12:40:37+00:00',
            'eTag': '9cbc7221e19bfbe2916e7ef0442ae2d9-2',
            'size': 18120358,
            'storageClass': 'STANDARD',
            'fileId': '005065260',
            'label': 'https://marbleb multimedia library nd edu/Aleph//CROCKER 1 01 Universi pdf',
            'sourceType': 'S3',
            'source': 'libnd-smb-marble',
            'path': 'Aleph/005065260/CROCKER_1_01_Universi.tif',
            'sourceBucketName': 'libnd-smb-marble',
            'sourceFilePath': 'Aleph/005065260/CROCKER_1_01_Universi.pdf',
            'mimeType': 'image/tiff',
            'filePath': 'Aleph/005065260/CROCKER_1_01_Universi.tif',
            'objectFileGroupId': '005065260',
            'imageGroupId': '005065260',
            'mediaResourceId': 'Aleph%2F005065260%2FCROCKER_1_01_Universi',
            'mediaServer': 'https://image-iiif.library.nd.edu/iiif/2'
        }
        self.assertEqual(actual_results, expected_results)


if __name__ == '__main__':
    unittest.main()
