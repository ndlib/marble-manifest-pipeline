#
# note this is not a complete script it is missing some requiree parameter overrides
# it is only able to do an update.
# this also requires blueprints
#
templatePath='deploy/cloudformation/manifest-pipeline-pipeline.yml'
templatePath='../marble-blueprints/deploy/cloudformation/manifest-pipeline-pipeline.yml'

export S3_BUCKET=testlibnd-cf
export SENTRY_DSN=https://136d489c91484b55be18e0a28d463b43@sentry.io/1831199
export SSM_KEY_BASE=/all/new-csv

stackName='marble-manifest-deployment'
aws cloudformation deploy \
  --capabilities CAPABILITY_NAMED_IAM \
  --region us-east-1 \
  --stack-name $stackName \
  --template-file $templatePath \
  --parameter-overrides \
      SentryDsn=https://136d489c91484b55be18e0a28d463b43@sentry.io/1831199 \
      RBSCS3ImageBucketName="rbsc-test-files"

      # RBSCS3ImageBucketName="rbsc-test-files"\
      # ConfigurationRepoBranchName="bump_python_version"
