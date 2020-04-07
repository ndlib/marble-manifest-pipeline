# read_batch_ingets_combined_csv.py
import csv


def read_batch_ingest_combined_csv(filename):
    csvfile = open(filename, "r")
    json_result = []
    reader = csv.DictReader(csvfile)
    for row in reader:
        node = {}
        for item in row:
            node[item] = row[item]
        json_result.append(node)
    return json_result
