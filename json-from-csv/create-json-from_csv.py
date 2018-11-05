#!/usr/bin/env python

from processCsv import processCsv

#start of main execution

csvSet = processCsv()

csvSet.buildJson()

#print resulting json to STDOUT
print(csvSet.dumpJson())
