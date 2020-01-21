PROGNAME=$0

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

usage() {
  cat << EOF >&2
Usage: $PROGNAME stackname path-to-blueprints

Deployes the pipeline to the stage
Note: Ensure you have an env variable S3_BUCKET as the bucket to deploy to.

EOF
  exit 1
}
``
stackname=$1
export BLUEPRINTS_DIR=$2

if [ -z ${S3_BUCKET+x} ]; then
  usage
  exit 1
fi

./scripts/codebuild/install.sh  ||  { exit 1; }
./scripts/codebuild/pre_build.sh ||  { exit 1; }
./scripts/codebuild/build.sh ||  { exit 1; }
./scripts/codebuild/post_build.sh ||  { exit 1; }

aws cloudformation deploy --template-file output.yml --stack-name $stackname \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides AppConfigPath="/all/$stackname" \
    ContainerImageUrl="333680067100.dkr.ecr.us-east-1.amazonaws.com/marbl-image-1cs1q74l7njir:latest" \
    HostnamePrefix=$stackname \
    ImageServerHostname="/all/stacks/marble-image-service-prod/hostname" \
    MarbleProcessingKeyPath="/all/marble-data-processing/test" \
    SentryDsn="https://136d489c91484b55be18e0a28d463b43@sentry.io/1831199" \
  || { echo "${RED} FAILED FAILED LOOK UP ^${NC}"; exit 1; }

rm -rf output.yml


echo "${GREEN} SUCCESS! ${NC}"
