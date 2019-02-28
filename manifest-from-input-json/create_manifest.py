# Creates json manifest based on appropriately structured input json.
# to run, navigate to directory /manifest-from-input-json and execute command
#  python3 create_manifest.py ../example/ example-input.json ./

# parameters:
#   - input directory (../example/)
#   - file to process (example-input.json)
#   - output directory (optional) - if not included, file is created in input

from processJson import processJson
import sys


def handleError(errorMessage):
    sys.exit(errorMessage)


# start of main execution

# instantiate a processing object
processSet = processJson()

# parameters required
if len(sys.argv) < 3:
    handleError(processSet.errorInputRequired())

# handle input parameters...
inputDirectory = sys.argv[1]
inputFile = sys.argv[2]
# file created in input directory if not specified
outputDirectory = inputDirectory
if len(sys.argv) > 3:
    outputDirectory = sys.argv[3]

# verify that the input file exists - exit on error
if processSet.verifyInput(inputDirectory, inputFile, outputDirectory) is False:
    handleError(processSet.error)

# generate json manifest
processSet.buildManifest()

# output manifest
processSet.printManifest()  # print to STDOUT
processSet.dumpManifest()  # create file in output directory
