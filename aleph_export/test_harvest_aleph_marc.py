# test_harvest_aleph_marc
""" test harvest_aleph_marc """

import unittest
from pathlib import Path
from harvest_aleph_marc import HarvestAlephMarc
from dependencies.pipelineutilities.pipeline_config import get_pipeline_config  # noqa: E402


class Test(unittest.TestCase):
    """ Class for test fixtures """
    def setUp(self):
        self.event = {"local": True}
        self.event['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
        self.config = get_pipeline_config(self.event)
        marc_records_url = "https://alephprod.library.nd.edu/aleph_tmp/marble.mrc"
        self.harvest_marc_class = HarvestAlephMarc(self.config, self.event, marc_records_url)

    def test_1_return_csv_from_json(self):
        """ Return csv from json. """
        processed_records_count = self.harvest_marc_class.process_marc_records_from_stream(True)
        self.assertTrue(processed_records_count == 1)


def suite():
    """ define test suite """
    return unittest.TestLoader().loadTestsFromTestCase(Test)


if __name__ == '__main__':
    suite()
    unittest.main()
