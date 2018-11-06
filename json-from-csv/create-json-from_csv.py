#!/usr/bin/env python

from processCsv import processCsv
import sys

#start of main execution

# instantiate a processing object
csvSet = processCsv()

# verify the input files. Use the first command line argument,
# if provided, as the directory where the files reside
# (The default is '.')
csvDirectory = '.'

# if directory passed as commnd line arg, use it
if len(sys.argv) > 1:
    csvDirectory = sys.argv[1]

# verify that the main and sequnces files exist- exit on error
if csvSet.verifyCsvExist(csvDirectory) == False:
    print('%s \n' % csvSet.printCsvError())
    sys.exit(1)

# generate a json
csvSet.buildJson()

#print resulting json to STDOUT
print(csvSet.dumpJson())

