#!/bin/bash
BUCKETS_TO_DELETE="marbleb-prod-foundation-logbucketcc3b17e8-1v5on9uapr1bs marbleb-prod-foundation-publicbucketa6745c15-15bllc1u482ye marbleb-prod-foundation-SharedLogGroup74BE6F74-MrLExv7G6tK8"
for bucket in $BUCKETS_TO_DELETE; do
  echo "Deleteing $bucket"
  aws s3 rm s3://${bucket} --recursive
  aws s3api delete-bucket --bucket ${bucket} --region us-east-1
done
