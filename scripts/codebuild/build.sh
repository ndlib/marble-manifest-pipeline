echo "Input File"
echo $INPUT_FILE
echo "S3 Bucket"
echo $S3_BUCKET
echo "Package"
aws cloudformation package --template-file deploy/resources.yml --s3-bucket $S3_BUCKET --output-template-file output.yml
