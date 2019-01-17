import unittest, json, os, boto3
from processCsv import processCsv
from collections import OrderedDict

class TestProcessCsv(unittest.TestCase):
    def setUp(self):
        event_id = 'sample'
        process_bucket = os.environ['PROCESS_BUCKET']
        manifest_bucket = os.environ['MANIFEST_BUCKET']
        self.csvSet = processCsv(event_id,process_bucket,manifest_bucket)
        pass

    def test_noCsvFound(self):
        self.csvSet.config['main-csv'] = 'non-existant-file'
        self.assertFalse(self.csvSet.verifyCsvExist())

    def test_CsvFound(self):
        self.assertTrue(self.csvSet.verifyCsvExist())

    def test_writeEventData(self):
        # write event to S3
        event = { 'event': { 'data': 'sample' } }
        self.csvSet.writeEventData(event)
        # verify file exist in S3
        s3 = boto3.resource('s3')
        remote_file = self.csvSet.config['process-bucket-read-basepath'] \
                             + '/' + self.csvSet.id + '/' \
                             + self.csvSet.config['event-file']
        for s3_obj in s3.Bucket(self.csvSet.config['process-bucket']).objects.filter(Prefix=remote_file):
            self.assertTrue(s3_obj.key == remote_file)
        # delete file from S3
        s3.Object(self.csvSet.config['process-bucket'], remote_file).delete()

    def test_readEventData(self):
        # write event to S3
        event = { 'event': { 'data': 'sample' } }
        self.csvSet.writeEventData(event)
        # verify file exist in S3
        json_data = self.csvSet.readEventData(event)
        self.assertTrue(json_data['event']['data'] == 'sample')
        # delete file from S3
        s3 = boto3.resource('s3')
        remote_file = self.csvSet.config['process-bucket-read-basepath'] \
                             + '/' + self.csvSet.id + '/' \
                             + self.csvSet.config['event-file']
        s3.Object(self.csvSet.config['process-bucket'], remote_file).delete()

    # def test_buildJson(self):
    #     self.csvSet.verifyCsvExist('.')
    #     self.csvSet.buildJson()
    #     with open('./test_result.json') as json_data:
    #         # The Ordered Dict hook preserves the pair ordering in the file for comparison
    #         result_json = json.load(json_data, object_pairs_hook=OrderedDict)
    #         json_data.close()
    #     self.assertEqual(self.csvSet.dumpJson(), json.dumps(result_json, indent=2))

    # def test_emptyJson(self):
    #     with open('./test_skeleton.json') as json_data:
    #         # The Ordered Dict hook preserves the pair ordering in the file for comparison
    #         empty_json = json.load(json_data, object_pairs_hook=OrderedDict)
    #         json_data.close()
    #     print self.csvSet.dumpJson()
    #     print empty_json
    #     self.assertEqual(self.csvSet.dumpJson(), json.dumps(empty_json, indent=2))

if __name__ == '__main__':
    unittest.main()
