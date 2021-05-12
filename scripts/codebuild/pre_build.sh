echo "[Pre-Build phase - run unit tests] `date` in `pwd`"

python -m unittest discover ./pipelineutilities/test  ||  { echo 'Pipeline Utilities Tests Failed' ; exit 1; }
python -m unittest discover ./aleph_export  ||  { echo 'Aleph Tests Failed' ; exit 1; }
python -m unittest discover ./archivesspace_export  ||  { echo 'ArchivesSpace Tests Failed' ; exit 1; }
python -m unittest discover ./curate_export  ||  { echo 'Curate Tests Failed' ; exit 1; }
python -m unittest discover ./metadata_rules  ||  { echo 'Metadata Rules Tests Failed' ; exit 1; }
python -m unittest discover ./museum_export  ||  { echo 'Museum Tests Failed' ; exit 1; }
python -m unittest discover ./object_files_api  ||  { echo 'Object FIles Tests Failed' ; exit 1; }
