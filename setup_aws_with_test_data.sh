
# read the stage process bucket.
bucket=manifestpipeline-dev-processbucket-1vtt3jhjtkg21

# copy the example data to the bucket.
aws s3 cp example/main.csv s3://$bucket/2018_example_001/main.csv
aws s3 cp example/sequence.csv s3://$bucket/2018_example_001/sequence.csv
aws s3 cp example/009_output.tif s3://$bucket/2018_example_001/images/009_output.tif
aws s3 cp example/046_output.tif s3://$bucket/2018_example_001/images/046_output.tif
aws s3 cp example/2018_009.jpg s3://$bucket/2018_example_001/images/2018_009.jpg
aws s3 cp example/2018_049_004.jpg s3://$bucket/2018_example_001/images/2018_049_004.jpg
