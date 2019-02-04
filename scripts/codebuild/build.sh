echo "[Build phase] `date` in `pwd`"

echo "S3 Bucket"
echo $S3_BUCKET

echo "Copy the resource.yml to this project so it can run"
# cp $BLUPRINTS_DIRECTORY/deploy/cloudformation/resources.yml $CODE_DIRECTORY/
wget https://raw.githubusercontent.com/ndlib/mellon-manifest-pipeline/changes-to-cf-to-make-it-pipeline-ready/deploy/resources.yml

echo "Aws Package Command"
echo "aws cloudformation package --template-file resources.yml --s3-bucket $S3_BUCKET --output-template-file output.yml"
aws cloudformation package --template-file resources.yml --s3-bucket $S3_BUCKET --output-template-file output.yml
