echo "[Pre-Build phase] `date` in `pwd`"

echo "Copy the manifest-pipeline.yml to this project so it can run"
echo "$BLUEPRINTS_DIR/deploy/cloudformation/manifest-pipeline.yml"
cp $BLUEPRINTS_DIR/deploy/cloudformation/manifest-pipeline.yml ./

# Note:  We will need to do something differently on the following line, since unittest traverses the library, and tries to run a test on pyvips - which fails
pip install pyvips -t ./test
python -m unittest discover  ||  { echo 'Auto Tests Failed' ; exit 1; }
