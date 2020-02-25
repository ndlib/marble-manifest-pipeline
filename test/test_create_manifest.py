import unittest
import json
import os
import sys
from test.test_utils import load_data_for_test
from test.test_utils import debug_json
from pathlib import Path
where_i_am = os.path.dirname(os.path.realpath(__file__))
sys.path.append(where_i_am + "/../pipelineutilities")
sys.path.append(where_i_am + "/../process_manifest/")
from pipelineutilities.csv_collection import load_csv_data
from pipelineutilities.pipeline_config import get_pipeline_config
from iiifCollection import iiifCollection

base_config = {}
base_config['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
base_config['local'] = True
config = get_pipeline_config(base_config)


class TestCreateManifest(unittest.TestCase):
    def setUp(self):
        self.ids = [
            # 'collection-small',
            'item-one-image-archivespace',
            'item-one-image-embark',
            # 'item-multiple-images',
            # 'item-minimal-data'
        ]
        pass

    def test_buildJson(self):
        for id in self.ids:
            print("Testing id, {}".format(id))
            data = load_data_for_test(id)

            parent = load_csv_data(id, config)
            iiif = iiifCollection(config, parent)
            manifest = iiif.manifest()

            debug_json(data['manifest_json'], manifest)

            manifest_json = "".join(json.dumps(data['manifest_json'], sort_keys=True).split())
            result_json = "".join(json.dumps(manifest, sort_keys=True).split())

            self.assertEqual(result_json, manifest_json)

    def test_addProvider(self):
        tests = [
            {"provider": "rbsc", "result": "https://rarebooks.library.nd.edu/using"},
            {"provider": "rare", "result": "https://rarebooks.library.nd.edu/using"},
            {"provider": "mrare", "result": "https://rarebooks.library.nd.edu/using"},
            {"provider": "embark", "result": "https://sniteartmuseum.nd.edu/about-us/contact-us/"},
            {"provider": "museum", "result": "https://sniteartmuseum.nd.edu/about-us/contact-us/"},
            {"provider": "unda", "result": "http://archives.nd.edu/about/"}
        ]

        parent = load_csv_data("item-one-image-embark", config)
        for test in tests:
            parent.object['repository'] = test.get("provider")
            iiif = iiifCollection(config, parent)
            iiif.document.add_provider()
            self.assertEqual(test.get("result"), iiif.document.manifest_hash['provider'].get('id'))

        del parent.object['repository']
        iiif = iiifCollection(config, parent)
        iiif.document.add_provider()
        self.assertEqual("not here", iiif.document.manifest_hash.get('provider', "not here"))


if __name__ == '__main__':
    unittest.main()
