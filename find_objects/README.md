
## Run Locally.
Define Parameter Store paths:
  * export SSM_KEY_BASE=/all/manifest-pipeline-v3
  * export SSM_MARBLE_DATA_PROCESSING_KEY_BASE=/all/marble-data-processing/test (or /prod)

Run each of the following modules in sequence:
  cd find_objects
  python -c 'from handler import *; test()'

  cd ../find_images_for_objects
  python -c 'from handler import *; test()'

  cd ../send_objects_to_pipeline
  python -c 'from handler import *; test()'

Results are written to the process bucket
