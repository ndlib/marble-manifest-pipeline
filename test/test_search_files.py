import unittest
from shared.search_files import id_from_url, url_can_be_harvested, file_should_be_skipped, is_jpg


example_ids = {
    'https://rarebooks.nd.edu/digital/civil_war/letters/images/caley/5024-34.a.150.jpg': {'id': '5024-', 'group': '34', 'label': 'a'},
    'https://rarebooks.library.nd.edu/collections/ead_xml/images/MSN-EA_5026/MSN-EA_5026-20.a.150.jpg': {'id': 'MSN-EA_5026-', 'group': '20', 'label': 'a'},
    'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-016-c2.jpg': {'id': 'BPP_1001-', 'group': '016', 'label': 'c2'},
    'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-018.jpg': {'id': 'BPP_1001-', 'group': '018', 'label': ''},
    'https://rarebooks.nd.edu/digital/colonial_american/records/images/massachusetts/swansea/2718-05.a.150.jpg': {'id': '2718-', 'group': '05', 'label': 'a'},
    'https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8006-1-B/images/MSN-EA_8006-01-B-00a.jpg': {'id': 'MSN-EA_8006-', 'group': '01', 'label': 'B 00a'},
    'https://rarebooks.library.nd.edu/digital/bookreader/CodeLat_b04/images/CodeLat_b04-000a-front_cover.jpg': {'id': 'CodeLat_b04-', 'group': '', 'label': '000a front cover'} ,
    'https://rarebooks.nd.edu/digital/civil_war/papers_personal/images/edwards/1004-51.a_cvr.150.jpg': {'id': '1004-', 'group': '51', 'label': 'a cvr'},
    'https://rarebooks.nd.edu/digital/civil_war/letters/images/reeves/5012-23.bc.150.jpg': {'id': '5012-', 'group': '23', 'label': 'bc'},
    'https://rarebooks.nd.edu/digital/civil_war/diaries_journals/images/boardman/8000-00_cover_outside.150.jpg': {'id': '8000-', 'group': '00', 'label': 'cover outside'},
    'https://rarebooks.nd.edu/digital/civil_war/diaries_journals/images/arthur/8001-01.150.jpg': {'id': '8001-', 'group': '01', 'label': ''},
    'https://rarebooks.nd.edu/digital/civil_war/diaries_journals/images/cline/8007-000a.150.jpg': {'id': '8007-', 'group': '000', 'label': 'a'},
    'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-299-part1.jpg': {'id': 'BPP_1001-', 'group': '299', 'label': 'part1'},
}

temp_ids_example = {
    'https://rarebooks.library.nd.edu/collections/ead_xml/images/MSN-EA_5026/MSN-EA_5026-20.a.150.jpg': 'https://rarebooks.library.nd.edu/collections/ead_xml/images/MSN-EA_5026/MSN-EA_5026-20',
    'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-016-c2.jpg': 'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-016',
    'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-018.jpg': 'https://rarebooks.library.nd.edu/collections/ead_xml/images/BPP_1001/BPP_1001-018',
    'https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8006-1-B/images/MSN-EA_8006-01-B-00a.jpg': 'https://rarebooks.library.nd.edu/digital/bookreader/MSN-EA_8006-1-B/images/MSN-EA_8006',
    'https://rarebooks.library.nd.edu/digital/bookreader/CodeLat_b04/images/CodeLat_b04-000a-front_cover.jpg': 'https://rarebooks.library.nd.edu/digital/bookreader/CodeLat_b04/images/CodeLat_b04',
    'https://rarebooks.library.nd.edu/digital/bookreader/El_Duende/images/El_Duende_5_000003.jpg': 'https://rarebooks.library.nd.edu/digital/bookreader/El_Duende/images/El_Duende',
    'https://rarebooks.library.nd.edu/digital/bookreader/MSS_CodLat_e05/images/MSS_CodLat_e05-084r.jpg': 'https://rarebooks.library.nd.edu/digital/bookreader/MSS_CodLat_e05/images/MSS_CodLat_e05',
    'https://rarebooks.library.nd.edu/digital/bookreader/images/CodLat_c03/MSS_CodLat_c3_098r.jpg': 'https://rarebooks.library.nd.edu/digital/bookreader/images/CodLat_c03/MSS_CodLat_c3',
    'https://rarebooks.library.nd.edu/digital/bookreader/Newberry-Case_MS_181/images/Newberry-Case_MS_181-999d.jpg': 'https://rarebooks.library.nd.edu/digital/bookreader/Newberry-Case_MS_181/images/Newberry-Case_MS_181',

}

valid_urls = [
    'https://rarebooks.library.nd.edu/path/to/file.jpg',
    'http://rarebooks.library.nd.edu/path/to/file.jpg',
]

invalid_urls = [
    'https://rarebooks.nd.edu/path/to/file.jpg',
    'https://www.google.com/path/to/file.jpg'
]

skipped_files = [
    '._filename.jpg',
    'filename.100.jpg',
]

valid_files = [
    'filename.150.jpg',
    'filename.150.jpeg',
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
            print(url)
            self.assertTrue(file_should_be_skipped(url))

        for url in valid_files:
            self.assertFalse(file_should_be_skipped(url))

    def test_is_jpg(self):
        tests = [
            'filename.jpg',
            'filename.jpeg',
            'filename.jpEg',
            'filename.JPG',
        ]

        for test in tests:
            self.assertTrue(is_jpg(test))


if __name__ == '__main__':
    unittest.main()
