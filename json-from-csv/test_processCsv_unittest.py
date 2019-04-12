import unittest
import json
from processCsv import processCsv


class TestProcessCsv(unittest.TestCase):
    def setUp(self):
        event_id = 'sample'
        config = {
            "image-server-base-url": "https://image-server.library.nd.edu:8182/iiif/2",
            "manifest-server-base-url": "https://manifest.nd.edu/",
            "process-bucket": "manifestpipeline-dev-processbucket-1vtt3jhjtkg21",
            "process-bucket-read-basepath": "process",
            "process-bucket-write-basepath": "finished",
            "image-server-bucket": "manifestpipeline-dev-processbucket-1vtt3jhjtkg21",
            "image-server-bucket-basepath": "images",
            "manifest-server-bucket": "manifestpipeline-dev-processbucket-1vtt3jhjtkg21",
            "manifest-server-bucket-basepath": "manifest",
            "notify-on-finished": "notify@email.com",
            "canvas-default-height": 2000,
            "canvas-default-width": 2000,
            "event-file": "event.json"
          }

        f = open('../example/main.csv', 'r')
        main_csv = f.read()
        f.close()

        f = open('../example/sequence.csv', 'r')
        sequence_csv = f.read()
        f.close()

        self.csvSet = processCsv(event_id, config, main_csv, sequence_csv)
        pass

    def test_buildJson(self):
        self.csvSet.buildJson()
        with open('./test_result.json') as json_data:
            # The Ordered Dict hook preserves the pair ordering in the file for comparison
            result_json = json.load(json_data)
            json_data.close()
        self.assertEqual(self.csvSet.dumpJson(), json.dumps(result_json, indent=2))


if __name__ == '__main__':
    unittest.main()
