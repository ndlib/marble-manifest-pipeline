from os import listdir
from os.path import isfile, join
import os
import re
import pprint
from shutil import copyfile, rmtree
import json


def main():
    data = {}
    new_item = {}
    pp = pprint.PrettyPrinter(indent=4)

    print("")
    print("-------------------")
    print("Add to what directory?")
    directory = input(":> ")

    data = load_manifest(directory)
    print(data)

    print("")
    print("-------------------")
    print("What is the item id?")
    new_item["id"] = input(":> ")
    if not new_item["id"]:
        print("Invalid systen id")
        exit()

    print("")
    print("-------------------")
    print("What directory do you want to add?")
    possible_directories = look_for_directory(input(":> "))
    new_item["current_directory"] = select_path(possible_directories)

    print(f"Selected Directory: {new_item.get('current_directory')}")
    print("...")
    print("...Searching directory Please Wait...")
    new_item["files"] = get_directory_files(new_item["current_directory"])
    if not new_item["files"]:
        print("Unable to find directory")
        exit()

    print("")
    print("-------------------")
    print("...Copying images please wait...")
    copy_files(id, new_item)

    data["items"][new_item["id"]].append(new_item)

    print("")
    print("-------------------")
    print("Writing JSON")
    write_json(data)
    print("Done")
    input(":> Press Enter to Continue")

    print("")
    print("-------------------")
    print("All Data")
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(data)

    print("Done")


def load_manifest(id):
    path = os.path.join("./fixit/", id)

    with open(os.path.join(path, "manifest.json")) as json_file:
        return json.load(json_file)


def look_for_directory(test_directory):
    possible_matches = []
    all_directories = load_directories()
    for path in all_directories:
        if test_directory in path:
            possible_matches.append(path)

    return possible_matches


def select_path(possible_directories):
    print("Select the directory you are searching for:")
    for index, path in enumerate(possible_directories):
        print(" " + (str(index + 1)) + ": " + path)

    selection = input(":> ")
    return possible_directories[int(selection) - 1]


def get_directory_files(current_directory):
    files = [join(current_directory, f) for f in listdir(current_directory) if can_copy_file(current_directory, f)]

    return {
        "id": item_id,
        "path": current_directory,
        "files": files
    }


def can_copy_file(path, f):
    return (
        (
            isfile(join(path, f)) and
            not re.match(r"(^.*[.]100[.]jpg$|^.*[.]072[.]jpg)", f) and
            re.match(r"^.*[.]jpg$", f) and
            not re.match(r"^[.][_]", f)
        )
        or
        (
            isfile(join(path, f)) and
            re.match(r".*[.]tif", f) and
            not re.match(r"^[.][_]", f)
        )
    )


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


def copy_files(directory, new_item):
    path = os.path.join("./fixit/", directory)

    for file in files["files"]:
        copyfile(file, os.path.join(path, os.path.basename(file)))


def rename_files(data):
    path = os.path.join("./fixit/", data["rbsc_id"])
    update_files = []
    for file in data["files"]:
        existing_filename = os.path.basename(file)
        newname = os.path.basename(file)

        newname = newname.replace(".150.jpg", ".jpg")
        newname = newname.replace(data["replacement_pattern"], data["rbsc_id"])
        if newname != existing_filename:
            os.rename(os.path.join(path, existing_filename), os.path.join(path, newname))

        update_files.append({
            "file": newname,
            "label": label_from_name(newname, data["rbsc_id"])
        })
    data["files"] = update_files


def label_from_name(file, replace_id):
    file = file.replace(replace_id, "")
    file = file.replace(".jpg", "")
    file = file.replace(".tif", "")
    file = file.replace("-", " ")
    file = file.replace("_", " ")
    file = file.replace(".", " ")
    file = re.sub(' +', ' ', file)
    file = re.sub('^ ', '', file)
    return file


def clean_up(data):
    path = os.path.join("./fixit/", data["rbsc_id"])
    if os.path.isdir(path):
        rmtree(path)


def add_to_rbsc_to_id(data):
    try:
        with open('./rbsc_to_source.json', 'w') as json_file:
            map = json.load(json_file)
    except IOError:
        map = {
            "rbsc_to_source": {},
            "source_to_rbsc": {}
        }

    map["rbsc_to_source"][data["rbsc_id"]] = data["source_system_id"]
    map["source_to_rbsc"][data["source_system_id"]] = data["rbsc_id"]

    with open('./rbsc_to_source.json', 'w') as outfile:
        json.dump(map, outfile, indent=4)


def write_json(data):
    path = os.path.join("./fixit/", data["rbsc_id"])
    with open(path + '/manifest.json', 'w') as outfile:
        json.dump(data, outfile, indent=4)


def load_directories():
    with open('./directories.json') as json_file:
        return json.load(json_file)


if __name__ == "__main__":
    main()

# MSN-CW_8010
#
