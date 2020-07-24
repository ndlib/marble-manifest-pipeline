echo "[Pre-Build phase] `date` in `pwd`"

echo "Copy the manifest-pipeline.yml to this project so it can run"
echo "$BLUEPRINTS_DIR/deploy/cloudformation/manifest-pipeline.yml"
cp $BLUEPRINTS_DIR/deploy/cloudformation/manifest-pipeline.yml ./

python -m unittest discover ./test  ||  { echo 'Auto Tests Failed' ; exit 1; }
python -m unittest discover ./pipelineutilities/test  ||  { echo 'Auto Tests Failed' ; exit 1; }
python -m unittest discover ./aleph_export  ||  { echo 'Auto Tests Failed' ; exit 1; }
python -m unittest discover ./museum_export  ||  { echo 'Auto Tests Failed' ; exit 1; }
python -m unittest discover ./archivesspace_export  ||  { echo 'Auto Tests Failed' ; exit 1; }
python -m unittest discover ./curate_export  ||  { echo 'Auto Tests Failed' ; exit 1; }
python -m unittest discover ./metadata_rules  ||  { echo 'Auto Tests Failed' ; exit 1; }
python -m unittest discover ./collections_api  ||  { echo 'Auto Tests Failed' ; exit 1; }
python -m unittest discover ./object_files_api  ||  { echo 'Auto Tests Failed' ; exit 1; }
