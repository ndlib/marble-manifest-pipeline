echo "[Build phase] `date` in `pwd`"

echo "S3 Bucket"
echo $S3_BUCKET

echo "Aws Package Command"
echo "aws cloudformation package --template-file deploy/resources.yml --s3-bucket $S3_BUCKET --output-template-file output.yml"
aws cloudformation package --template-file deploy/resources.yml --s3-bucket $S3_BUCKET --output-template-file output.yml
