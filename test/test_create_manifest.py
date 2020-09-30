import _set_test_path  # noqa
import unittest
import json
from test.test_utils import load_data_for_test
from test.test_utils import debug_json
from pathlib import Path
from load_standard_json import load_standard_json
from pipeline_config import setup_pipeline_config
from iiifManifest import iiifManifest
from MetadataMappings import MetadataMappings


base_config = {}
base_config['local-path'] = str(Path(__file__).parent.absolute()) + "/../example/"
base_config['local'] = True
config = setup_pipeline_config(base_config)


class TestCreateManifest(unittest.TestCase):
    def setUp(self):
        self.ids = [
            "1999.024",
            "1952.019",
            "002097132",
            "004862474",
            "004476467",  # has HESB
            "MSNCOL8501_EAD",
            "pdf"
        ]
        pass

    def test_build_nd_json(self):
        for id in ["1999.024", "1952.019", "MSNCOL8501_EAD", "pdf"]:
            # print("Testing id, {}".format(id))
            parent = load_standard_json(id, config)
            mapping = MetadataMappings(parent)
            iiif = iiifManifest(config, parent, mapping)
            manifest = iiif.manifest()
            # print("id=", id)
            # current_path = str(Path(__file__).parent.absolute())
            # with open(current_path + '/../example/{}/manifest.json'.format(id), "w") as output_file:
            #     json.dump(manifest, output_file, indent=2, ensure_ascii=False)
            data = load_data_for_test(id)
            # print("data = ", data)
            debug_json(data['manifest_json'], manifest)
            manifest_json = "".join(json.dumps(data['manifest_json'], sort_keys=True).split())
            result_json = "".join(json.dumps(manifest, sort_keys=True).split())
            self.assertEqual(result_json, manifest_json, msg="%s did not match see test/debug for output" % (id))

    def test_manifest_that_is_a_pdf_without_a_mime_type(self):
        data = load_data_for_test('pdf')
        # remove the mime type
        parent = load_standard_json('pdf', config)
        parent.object['items'][0]['mimeType'] = ''

        mapping = MetadataMappings(parent)
        iiif = iiifManifest(config, parent, mapping)
        manifest = iiif.manifest()
        # debug_json(data['manifest_json'], manifest)
        manifest_json = "".join(json.dumps(data['manifest_json'], sort_keys=True).split())
        result_json = "".join(json.dumps(manifest, sort_keys=True).split())
        self.assertEqual(result_json, manifest_json)

    def test_addProvider(self):
        tests = [
            {"provider": "rare", "result": "https://rarebooks.library.nd.edu/using"},
            {"provider": "hesb", "result": "https://library.nd.edu"},
            {"provider": "museum", "result": "https://sniteartmuseum.nd.edu/about-us/contact-us/"},
            {"provider": "unda", "result": "http://archives.nd.edu/about/"}
        ]

        # parent = load_csv_data("item-one-image-embark", config)
        parent = load_standard_json("item-one-image-embark", config)
        # print("parent = ", parent)
        for test in tests:
            parent.object['repository'] = test.get("provider")
            mapping = MetadataMappings(parent)
            iiif = iiifManifest(config, parent, mapping)
            iiif.add_provider()
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
        # parent = load_csv_data("item-one-image-embark", config)
        parent = load_standard_json("item-one-image-embark", config)
        parent.object["uniqueIdentifier"] = 1999.2312

        mapping = MetadataMappings(parent)
        iiif = iiifManifest(config, parent, mapping)
        iiif.metadata_array()
        self.assertEqual("1999.2312", iiif.manifest_hash.get("metadata")[4].get("value").get("en")[0])


if __name__ == '__main__':
    unittest.main()
