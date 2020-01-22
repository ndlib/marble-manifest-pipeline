import json
from pathlib import Path


def load_data_for_test(id):
    data = {}
    current_path = str(Path(__file__).parent.absolute())

    #with open(current_path + "/../example/{}/image-data.json".format(id), 'r') as input_source:
    #    data['image_data'] = json.load(input_source)
    #input_source.close()

    with open(current_path + '/../example/test_manifests/{}.json'.format(id), 'r') as input_source:
        data['manifest_json'] = json.load(input_source)
    input_source.close()

    return data


def debug_json(tested, result):
    tested = json.dumps(tested, sort_keys=True, indent=2)
    result = json.dumps(result, sort_keys=True, indent=2)

    f = open("./test/debug/test.json", "w")
    f.write(tested)
    f.close()

    f = open("./test/debug/result.json", "w")
    f.write(result)
    f.close()


def load_img_data_for_test():
    current_path = str(Path(__file__).parent.absolute())
    return {
        'woman': {
            'path': current_path + "/../example/2018_049_004.jpg",
            'height': 2274,
            'width': 3000,
        },
        'cube': {
            'path': current_path + "/../example/009_output.tif",
            'height': 3000,
            'width': 2395,
        }
    }
