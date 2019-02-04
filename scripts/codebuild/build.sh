echo "[Build phase] `date` in `pwd`"

echo "S3 Bucket"
echo $S3_BUCKET

echo "Copy the resource.yml to this project so it can run"
cp $CODEBUILD_SRC_DIR_ConfigCode/deploy/cloudformation/manifest-pipeline.yml $CODEBUILD_SRC_DIR/

echo "Aws Package Command"
echo "aws cloudformation package --template-file manifest-pipeline.yml --s3-bucket $S3_BUCKET --output-template-file output.yml"
aws cloudformation package --template-file manifest-pipeline.yml --s3-bucket $S3_BUCKET --output-template-file output.yml
