from pathlib import Path
import os
import json


def main():
    output = []
    search_paths = [
        "/Volumes/libnd-smb-rbsc/collections/ead_xml/images",
        "/Volumes/libnd-smb-rbsc/digital/"
    ]

    for search_path in search_paths:
        for path in Path(search_path).rglob('*.jpg'):
            path_part = os.path.dirname(path)
            if path_part not in output:
                output.append(path_part)
        for path in Path(search_path).rglob('*.tif'):
            path_part = os.path.dirname(path)
            if path_part not in output:
                output.append(path_part)

    with open('directories.json', 'w') as outfile:
        json.dump(output, outfile)

    print(output)


if __name__ == "__main__":
    main()
