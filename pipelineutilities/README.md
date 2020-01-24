# Basic Instructions
For now until we figure something better out.

## Changing utilities
At the root of pipelineutilities
Run:
`
  python setup.py bdist_wheel
`


## Installing in a lambda.
`
pip install path/to/root/ofpipeline
`

Note that this will require you to delete and reinstall the package because we are not versioning it.
You can see on the lambda install directories to see what is being done where we remove the files then readd.

## Running tests.
From the root of the project run_all_tests.sh

This will do a lot of installation though so if you want to just run these
`
  python -m unittest discover ./pipelineutilities/test
`
