# mellon-manifest-pipeline

## Step 1: Generating a JSON file from CSV fles

The initial input to the manifest pipeline is two CSV files.

### First CSV file
The first CSV file contains the main metadata. It should end with *main.csv*

Detailed information about what these fields map to can be found [here](https://iiif.io/api/presentation/3.0/#36-values)

The following labels are expected:

* Label - The label for the image sequence
* Summary - A short description of the image sequence
* Rights - Licensing or Copyright information
* Attribution - Artist attribution
* unique_identifier - Id used for generating IIIF manifest
* Metadata_label - Used to specify metadata common to all images in the sequence
* Metadata_value - Used to specify metadata common to all images in the sequence
* Notify - The email(s) of individual(s) to be contacted by the pipeline
* SeeAlso_Label - An external, machine-readable resource such as an XML or RDF description that is related to the resource
* SeeAlso_Type - The type of the seeAlso resource
* SeeAlso_Format - The format of the seeAlso resource
* SeeAlso_Profile - The profile of the seeAlso resource
* Alternate_id_system - The id of a resource in an alternate system that relates to this resource
* Alternate_id_identifier - The id of alternate system
* Alternate_id_url - The url of the alternate system

An example file, *example-main.csv*, looks like this:

```

Label,Notify,Summary,Rights,Attribution,unique_identifier,Metadata_label,Metadata_value,SeeAlso_Id,SeeAlso_Type,SeeAlso_Format,SeeAlso_Label,SeeAlso_Profile,Alternate_id_system,Alternate_id_identifier,Alternate_id_url,Index_for_MARBLE
ANNUNCIATION (A PAIR OF PRELATE'S CUFFS),rdought1@nd.edu,ANNUNCIATION (A PAIR OF PRELATE'S CUFFS),"<a href=""http://rightsstatements.org/vocab/NoC-US/1.0/"" target=""_blank"">No Copyright - United States</a>",University of Notre Dame::Hesburgh Libraries::General,1982.072.001.a,Title,ANNUNCIATION (A PAIR OF PRELATE'S CUFFS),http://www.someurl.com/library/catalog/book1.xml,Dataset,text/xml,dataset from Curate ,https://example.org/profiles/bibliographic,Curate,R2345234534,http://www.thegoods.com/book1.html,YES
,,,,,,Creator,Italian,http://www.anotherurl.org/library/cata.log/mag1.xml,Dataset,text/xml,data from Snite  ,https://sample.org/profiles/biblio,,,,
,,,,,,,,,,,,,,,,
,,,,,,Classification,painting,,,,,,,,,
,,,,,,Media,"silk, metallic thread, mounted on leather",,,,,,,,,
,,,,,,Dimensions,6 1/2 x 11 in. (16.51 x 27.94 cm),,,,,,,,,
,,,,,,Accession Number,1982.072.001.a,,,,,,,,,
,,,,,,Repository,Snite,,,,,,,,,

```

### Second CSV file
The first CSV file contains the image metadata. It should end with *items.csv*

The following labels are expected:

* Filenames - The actual file name under the image directory
* Label - This file's Label
* Description - A file-specific description

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

The `create-json-from_csv` script will look for a main and items CSV located in a directory together, and use them to generate a JSON file to standard out (the commandline). By default, it looks in the current directory. If an argument is provided on the command line, it will look for the files in that directory instead.

An example:

Let's say that the directory *myCsvFiles* contains the above example main and sequence files, named *mycsv-main.csv* and  *mycsv-sequence.csv*.

The command: `create-json-from_csv myCsvFiles`

would produce the following output:

```
{
    "errors": [
      
    ],
    "creator": "sample@domain",
    "viewingDirection": "left-to-right",
    "metadata": [
      {
        "label": {
          "en": [
            "Summary"
          ]
        },
        "value": {
          "en": [
            "ANNUNCIATION (A PAIR OF PRELATE'S CUFFS)"
          ]
        }
      },
      {
        "label": {
          "en": [
            "Title"
          ]
        },
        "value": {
          "en": [
            "ANNUNCIATION (A PAIR OF PRELATE'S CUFFS)"
          ]
        }
      },
      {
        "label": {
          "en": [
            "Creator"
          ]
        },
        "value": {
          "en": [
            "Italian"
          ]
        }
      },
      {
        "label": {
          "en": [
            "Classification"
          ]
        },
        "value": {
          "en": [
            "painting"
          ]
        }
      },
      {
        "label": {
          "en": [
            "Media"
          ]
        },
        "value": {
          "en": [
            "silk, metallic thread, mounted on leather"
          ]
        }
      },
      {
        "label": {
          "en": [
            "Dimensions"
          ]
        },
        "value": {
          "en": [
            "6 1/2 x 11 in. (16.51 x 27.94 cm)"
          ]
        }
      },
      {
        "label": {
          "en": [
            "Accession Number"
          ]
        },
        "value": {
          "en": [
            "1982.072.001.a"
          ]
        }
      },
      {
        "label": {
          "en": [
            "Repository"
          ]
        },
        "value": {
          "en": [
            "Snite"
          ]
        }
      }
    ],
    "items": [
      {
        "file": "1982_072_001_a-v0001.jpg",
        "label": "072_001_a-v0001"
      },
      {
        "file": "1982_072_001_a-v0002.jpg",
        "label": "072_001_a-v0002"
      }
    ],
    "label": {
      "en": [
        "ANNUNCIATION (A PAIR OF PRELATE'S CUFFS)"
      ]
    },
    "requiredStatement": {
      "label": {
        "en": [
          "Attribution"
        ]
      },
      "value": {
        "en": [
          "University of Notre Dame::Hesburgh Libraries::General"
        ]
      }
    },
    "rights": "<a href="http://rightsstatements.org/vocab/NoC-US/1.0/" target="_blank">No Copyright - United States</a>",
    "unique-identifier": "1982.072.001.a",
    "homepage": [
      {
        "id": "http://www.thegoods.com/book1.html",
        "label": {
          "en": [
            "Curate - bitter"
          ]
        },
        "type": "Text",
        "format": "text/html"
      }
    ],
    "seeAlso": [
      {
        "id": "http://www.someurl.com/library/catalog/book1.xml",
        "type": "Dataset",
        "format": "text/xml",
        "profile": "https://example.org/profiles/bibliographic"
      },
      {
        "id": "http://www.anotherurl.org/library/cata.log/mag1.xml",
        "type": "Dataset",
        "format": "text/xml",
        "profile": "https://sample.org/profiles/biblio"
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


## Manifest Utility Script
This utility script will allow the user to do two specific task.
1. deploy local files to an s3 bucket and have the manifest pipeline process them
1. run an already processed manifest again

The details for starting each of these task is below

### Initial Running of Data
The script will take csv files and images and deploy them to s3 where they will be sent down the manifest pipeline for processing.

#### Prerequisites
1. an AWS bucket[sample-bucket]
1. CSV files in this directory structure in /tmp on the local machine
    1. tmp/[event]/main.csv
    1. tmp/[event]/sequence.csv
    1. tmp/[event]/images/[image files here]
1. Step Function ARN for manifest processing[sample-steps]

#### How to
Last but not least you'll need to create a text file with one event line you'd like to process again.

        events.txt
                TheJollyRoger
                RuinsOfBabylon
                Firefly
                EventHorizon
                SomeOtherEvent

Now to run the script:

```python3 manifest_util.py --run --bucket <BucketId> -s arn:aws:states:us-east-1:<AWS ID>:stateMachine:<StateMachineId> -e events.txt```

EX:

```python3 manifest_util.py --run --bucket mybucket -s arn:aws:states:us-east-1:1234567890:stateMachine:StateMachine-IH8SMHeRe -e events.txt```

### Rerunning Processed Data
The python script manifest_util.py will allow you to run processed data again.

#### Prerequisites
1. an AWS bucket[sample-bucket]
1. CSV files in this directory structure in [sample-bucket]
    1. finished/[event]/lastSuccessfullRun/main.csv
    1. finished/[event]/lastSuccessfullRun/sequence.csv
    1. finished/[event]/lastSuccessfullRun/images/[image files here]
1. Step Function ARN for manifest processing[sample-steps]

#### How to
Last but not least you'll need to create a text file with one event line you'd like to process again.

        events.txt
                TheJollyRoger
                RuinsOfBabylon
                Firefly
                EventHorizon
                SomeOtherEvent

Now to run the script:

```python3 manifest_util.py --rerun --bucket <BucketId> -s arn:aws:states:us-east-1:<AWS ID>:stateMachine:<StateMachineId> -e events.txt```

EX:

```python3 manifest_util.py --rerun --bucket mybucket -s arn:aws:states:us-east-1:1234567890:stateMachine:StateMachine-IH8SMHeRe -e events.txt```

# Local Development
## Prerequisites
For consistent coding standards install [flake8](http://flake8.pycqa.org/en/latest/index.html) via [pip](https://pypi.org/project/pip/)

`pip install -r dev-requirements.txt`

To run [flake8](http://flake8.pycqa.org/en/latest/index.html) manually

`flake8 json-from-csv/handler.py`

The projects custom linter configurations can be found in .flake8

Additional options can be found [here](http://flake8.pycqa.org/en/latest/user/options.html)

Various Editors and IDEs have plugins that work with this linter.
 * In ATOM install [linter-flake8](https://atom.io/packages/linter-flake8)
 * In Sublime install [SublimeLinter-flake8](https://github.com/SublimeLinter/SublimeLinter-flake8)
 * In VS Code modify these [config settings](https://code.visualstudio.com/docs/python/settings-reference#_flake8)

## Deployment
Run the script local-deploy providing the name of the stack you want to deploy and the
path to the mellon-blueprints repo

```bash
./local-deploy.sh manifest-pipeline-jon ../mellon-blueprints/
```
