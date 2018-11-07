import unittest, json
from processCsv import processCsv
from collections import OrderedDict

class TestProcessCsv(unittest.TestCase):
    def setUp(self):
        pass

    def test_emptyJson(self):
        self.csvSet = processCsv()
        with open('./test_skeleton.json') as json_data:
            # The Ordered Dict hook preserves the pair ordering in the file for comparison
            empty_json = json.load(json_data, object_pairs_hook=OrderedDict)
            json_data.close()
        self.assertEqual(self.csvSet.dumpJson(), json.dumps(empty_json, indent=2))

    def test_noCsvFound(self):
        self.csvSet = processCsv()
        self.assertFalse(self.csvSet.verifyCsvExist('./non-existent-directory'))

    def test_CsvFound(self):
        self.csvSet = processCsv()
        self.assertTrue(self.csvSet.verifyCsvExist('.'))

    def test_buildJson(self):
        self.csvSet = processCsv()
        self.csvSet.verifyCsvExist('.')
        self.csvSet.buildJson()
        with open('./test_result.json') as json_data:
            # The Ordered Dict hook preserves the pair ordering in the file for comparison
            result_json = json.load(json_data, object_pairs_hook=OrderedDict)
            json_data.close()
        self.assertEqual(self.csvSet.dumpJson(), json.dumps(result_json, indent=2))

if __name__ == '__main__':
    unittest.main()
