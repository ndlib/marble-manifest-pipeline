import unittest
import json
from test.test_utils import load_data_for_test
from test.test_utils import debug_json
from pathlib import Path
from csv_collection import load_csv_data
from pipeline_config import get_pipeline_config
from iiifManifest import iiifManifest
from MetadataMappings import MetadataMappings


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
            # print("data = ", data)
            parent = load_csv_data(id, config)
            mapping = MetadataMappings(parent)
            iiif = iiifManifest(config, parent, mapping)
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
            mapping = MetadataMappings(parent)
            iiif = iiifManifest(config, parent, mapping)
            iiif.add_provider()
            print(parent.object['level'])
            self.assertEqual(test.get("result"), iiif.manifest_hash['provider'][0].get('id'))

        # if there is no repository there is no result
        del parent.object['repository']
        mapping = MetadataMappings(parent)
        iiif = iiifManifest(config, parent, mapping)
        iiif.add_provider()
        self.assertEqual("not here", iiif.manifest_hash.get('provider', "not here"))
        # reset
        parent.object['repository'] = 'rbsc'

        # if the type is file we don't show anything either.
        parent.object['level'] = 'file'
        mapping = MetadataMappings(parent)
        iiif = iiifManifest(config, parent, mapping)
        iiif.add_provider()
        self.assertEqual("not here", iiif.manifest_hash.get('provider', "not here"))
        # reset
        parent.object['level'] = 'collection'

    def test_metadata_mappings(self):
        # it converts floats to strings
        parent = load_csv_data("item-one-image-embark", config)
        parent.object["uniqueIdentifier"] = 1999.2312

        mapping = MetadataMappings(parent)
        iiif = iiifManifest(config, parent, mapping)
        iiif.metadata_array()
        print(iiif.manifest_hash.get("metadata")[4].get("value").get("en")[0])
        self.assertEqual("1999.2312", iiif.manifest_hash.get("metadata")[4].get("value").get("en")[0])







if __name__ == '__main__':
    unittest.main()
