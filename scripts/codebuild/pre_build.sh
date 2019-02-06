echo "[Pre-Build phase] `date` in `pwd`"

echo "Copy the resource.yml to this project so it can run"
cp $CODEBUILD_SRC_DIR_ConfigCode/deploy/cloudformation/manifest-pipeline.yml $CODEBUILD_SRC_DIR/
