echo "[Pre-Build phase] `date` in `pwd`"

echo "Copy the manifest-pipeline.yml to this project so it can run"
echo "$CODEBUILD_SRC_DIR_ConfigCode/deploy/cloudformation/manifest-pipeline.yml"
cp $CODEBUILD_SRC_DIR_ConfigCode/deploy/cloudformation/manifest-pipeline.yml $CODEBUILD_SRC_DIR/
