import _set_path  # noqa
import unittest
from pipelineutilities.pipeline_config import test_required_fields


class TestPipelineConfig(unittest.TestCase):

    def test_test_required_fields(self):
        test = {
            'config-file': "value",
            'process-bucket': "value"
        }
        # no exception
        test_required_fields(test)

        with self.assertRaises(Exception):
            test_required_fields({'process-bucket': "value"})

        with self.assertRaises(Exception):
            test_required_fields({'config-file': "value"})
