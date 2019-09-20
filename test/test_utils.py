import json
from pathlib import Path


def load_data_for_test(self, id):
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
