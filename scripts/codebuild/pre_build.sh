echo "[Pre-Build phase] `date` in `pwd`"

echo "Copy the manifest-pipeline.yml to this project so it can run"
echo "$BLUEPRINTS_DIR/deploy/cloudformation/manifest-pipeline.yml"
cp $BLUEPRINTS_DIR/deploy/cloudformation/manifest-pipeline.yml ./

python -m unittest discover ./test  ||  { echo 'Auto Tests Failed' ; exit 1; }
python -m unittest discover ./pipelineutilities/test  ||  { echo 'Auto Tests Failed' ; exit 1; }
# removed for now because I, jon, am having trouble with the python modules.
# python -m unittest discover ./aleph_export  ||  { echo 'Auto Tests Failed' ; exit 1; }
