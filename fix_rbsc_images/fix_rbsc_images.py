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

    print("")
    print("-------------------")
    print("What is the item id?")
    item_id = input(":> ")
    if not item_id:
        print("Invalid systen id")
        exit()

    print("")
    print("-------------------")
    print("What is the current directory name of the images?")
    possible_direcories = look_for_directory(input(":> "))
    current_directory = select_path(possible_direcories)

    print(f"Selected Directory: {current_directory}")
    print("...")
    print("...Searching directory Please Wait...")
    data = get_directory_files(current_directory)
    if not data["files"]:
        print("Unable to find directory")
        exit()

    print("")
    print("-------------------")
    print("What is the pattern of the file to replace with the directory name?")
    print("If it is the same reenter the directory value")
    print(f"Example File: {os.path.basename(data['files'][0])}")

    data["replacement_pattern"] = validate_replacement_pattern(input(":> "), data)
    if not data["replacement_pattern"]:
        print("invalid replacement pattern")
        exit()

    print("")
    print("-------------------")
    print("All Data")
    pp = pprint.PrettyPrinter(indent=4)
    pp.pprint(data)

    input(":> Press Enter to Continue")

    print("")
    print("-------------------")
    print("...Copying images please wait...")
    clean_up(data)
    copy_files(data)
    print("Done")
    input(":> Press Enter to Continue")

    print("")
    print("-------------------")
    print("Renaming Files")
    rename_files(data)
    print("Done")
    input(":> Press Enter to Continue")

    print("")
    print("-------------------")
    print("Writing JSON")
    write_json(data)
    print("Done")
    input(":> Press Enter to Continue")

    print("")
    print("-------------------")
    print("Moving Back")
    print("Done")
    input(":> Press Enter to Continue")

    print("")
    print("-------------------")
    print("Cleaning Up")
    clean_up(data)

    print("Done")


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


def copy_files(data):
    new_path = os.path.join("./fixit/", data["rbsc_id"])
    os.makedirs(new_path)

    for file in data["files"]:
        copyfile(file, os.path.join(new_path, os.path.basename(file)))


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
