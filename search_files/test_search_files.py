import unittest
import search_files
#from search_files.search_files import get_matching_s3_objects

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
    'filename.100.jpg'
]

# date example we may remediated saved for now.
# 'https://rarebooks.nd.edu/digital/civil_war/records_military/images/bloodgood/1864_07_10.c.150.jpg]': ['1864_07_10', 'c'],
class TestSearchFiles(unittest.TestCase):

    def test_examples(self):
        for key in example_ids:
            output = search_files.parse_filename(key)
            self.assertEqual(output, example_ids[key])

    def test_url_can_be_harvested(self):
        for url in valid_urls:
            self.assertTrue(search_files.url_can_be_harvested(url))

        for url in invalid_urls:
            self.assertFalse(search_files.url_can_be_harvested(url))

    def test_file_should_be_skipped(self):
        for url in skipped_files:
            self.assertTrue(search_files.file_should_be_skipped(url))



if __name__ == '__main__':
    unittest.main()
