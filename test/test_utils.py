import json
from pathlib import Path


def load_data_for_test(id):
    data = {}
    current_path = str(Path(__file__).parent.absolute())

    with open(current_path + "/../example/{}/config.json".format(id), 'r') as input_source:
        data['config'] = json.load(input_source)
    input_source.close()

    with open(current_path + "/../example/{}/main.csv".format(id), 'r') as input_source:
        data['main_csv'] = input_source.read()
    input_source.close()

    with open(current_path + "/../example/{}/items.csv".format(id), 'r') as input_source:
        data['items_csv'] = input_source.read()
    input_source.close()

    with open(current_path + "/../example/{}/image-data.json".format(id), 'r') as input_source:
        data['image_data'] = json.load(input_source)
    input_source.close()

    with open(current_path + '/../example/{}/event.json'.format(id), 'r') as input_source:
        data['event_json'] = json.load(input_source)
    input_source.close()

    with open(current_path + '/../example/{}/manifest.json'.format(id), 'r') as input_source:
        data['manifest_json'] = json.load(input_source)
    input_source.close()

    return data


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
