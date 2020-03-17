from os import listdir
from os.path import isfile, join
import os
import re
import pprint
from shutil import copyfile, rmtree
import json


def main():
    data = {}
    pp = pprint.PrettyPrinter(indent=4)

    print("")
    print("-------------------")
    print("What is the source system?")
    print("  1: aleph")
    print("  2: archive space")
    data["source_system"] = validate_source_system(input(":> "))
    if not data["source_system"]:
        print("Invalid Source System")
        exit()

    print("")
    print("-------------------")
    print("What is the source system id?")
    data["id"] = validate_source_system_id(input(":> "))
    if not data["id"]:
        print("Invalid systen id")
        exit()

    print("")
    print("-------------------")
    print("What should the the id for the directory be?")
    data["rbsc_id"] = input(":> ")

    data["items"] = {}

    print("")
    print("Writing Manifest")
    write_json(data)

    print("Done")


def validate_source_system(source_system_input):
    if source_system_input == "1":
        return "aleph"
    elif source_system_input == "2":
        return "archivespace"

    return False


def validate_source_system_id(id):
    return id


def validate_replacement_pattern(replacement_pattern, data):
    return replacement_pattern


def write_json(data):
    path = os.path.join("./fixit/", data["rbsc_id"])
    os.makedirs(path)
    with open(path + '/manifest.json', 'w') as outfile:
        json.dump(data, outfile, indent=4)


def load_directories():
    with open('./directories.json') as json_file:
        return json.load(json_file)


if __name__ == "__main__":
    main()

# MSN-CW_8010
#
