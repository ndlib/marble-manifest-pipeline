# mellon-manifest-pipeline

## Step 1: Generating a JSON file from CSV fles

The initial input to the manifest pipeline is two CSV files.

### First CSV file
The first CSV file contains the main metadata. It should end with *main.csv*

The following labels are expected:

**Label** The label for the image sequence
**Description**  A short description of the image sequence
**Rights**  Licensing or Copyright information
**Attribution** Artist attribution
**Sequence_filename** **Not Used**
**Sequence_label** The name of the image sequence array in the metadata JSON
**Sequence_viewing_experience** the value should be *paged* or *individuals*
**unique_identifier** id used for generating IIIF manifest
**Metadata_label** on second and subsequent lines, and be used to specify metadata common to all images in the sequence
**Metadata_value** on second and subsequent lines, and be used to specify metadata common to all images in the sequence

An example file, *example-main.csv*, looks like this:

```

Label,Description,Rights,Attribution,Sequence_filename,Sequence_label,Sequence_viewing_experience,unique_identifier,Metadata_label,Metadata_value
Label,Description,rights,attribution,,sequency,paged,2018_example_001,,
,,,,,,,,Title,"Wunder der Verenbung"
,,,,,,,,Author(s),"Bolle, Fritz"
,,,,,,,,"Publication date","[1951]"
,,,,,,,,Attribution,""Welcome Library<br/>License: CC-BY-NC"

```

### Second CSV file
The first CSV file contains the image sequence  metadata. It should end with *sequence.csv*

The following labels are expected:

**Filenames** the actual file name under the image directory
**Label** This file's Label
**Description** a file-specific description

The order in which the files are entered, by row, demotes their order in the image sequence.

An example:

```

Filenames,Label,Description
009_output,009,
046_output.tif,046,
2018_009.jpg,2018 009,
2018_049_009.jpg,2018 049 009,"Look, a JPG file"

```

### CSV conversion script

The `create-json-from_csv` script will look for a main and sequence CSV located in a directory together, and use them to generate a JSON file to standard out (the commandline). By default, it looks in the current directory. If an argument is provided on the command line, it will look for the files in that directory instead.

An example:

Let's say that the directory *myCsvFiles* contains the above example main and sequence files, named *mycsv-main.csv* and  *mycsv-sequence.csv*.

The command: `create-json-from_csv myCsvFiles`

would produce the following output:

```
{
  "errors": [],
  "attribution": "attribution",
  "description": "Description",
  "iiif-server": "https://image-server.library.nd.edu:8182/iiif/2",
  "creator": "creator@email.com",
  "rights": "rights",
  "unique-identifier": "2018_example_001",
  "label": "Label",
  "sequences": [
    {
      "viewingHint": "paged",
      "pages": [
        {
          "file": "009_output",
          "label": "009"
        },
        {
          "file": "046_output.tif",
          "label": "046"
        },
        {
          "file": "2018_009.jpg",
          "label": "2018 009"
        },
        {
          "file": "2018_049_009.jpg",
          "label": "2018 049 009"
        }
      ],
      "label": "sequency"
    }
  ],
  "metadata": [
    {
      "value": "Wunder der Verenbung",
      "label": "Title"
    },
    {
      "value": "Bolle, Fritz",
      "label": "Author(s)"
    },
    {
      "value": "[1951]",
      "label": "Publication date"
    },
    {
      "value": "Welcome Library<br/>License: CC-BY-NC\"",
      "label": "Attribution"
    }
  ]
}
```
## Step 2: Generating a manifest from the JSON output of step 1

The `create_manifest.py` script receives three attributes, and creates a JSON manifest file based on appropriately structured input JSON file.

Parameters:
* input directory (../example/)
* file to process (example-input.json)
* output directory (optional) - if not included, file is created in the input directory

To run using the example file, navigate to directory /manifest-from-input-json and execute command
```python3 create_manifest.py ../example/ example-input.json ./```

A JSON manifest file will be created in the output directory, and the JSON output will also be printed.

## Rerunning Processed Data
The script rerun.py will allow you to run processed data again.

### Prerequisites
1. an AWS bucket[sample-bucket]
1. CSV files in this directory structure in [sample-bucket]
    1. finished/[event]/lastSuccessfullRun/main.csv
    1. finished/[event]/lastSuccessfullRun/sequence.csv
    1. finished/[event]/lastSuccessfullRun/images/[image files here]
1. Step Function ARN for manifest processing[sample-steps]

### How to
Last but not least you'll need to create a text file with one event line you'd like to process again.

        events.txt
                TheJollyRoger
                RuinsOfBabylon
                Firefly
                EventHorizon
                SomeOtherEvent

Now to run the script:

```python3 rerun.py --bucket <BucketId> -s arn:aws:states:us-east-1:<AWS ID>:stateMachine:<StateMachineId> -e events.txt```

EX:

```python3 rerun.py --bucket mybucket -s arn:aws:states:us-east-1:1234567890:stateMachine:StateMachine-IH8SMHeRe -e events.txt```