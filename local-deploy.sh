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

./scripts/codebuild/install.sh
./scripts/codebuild/pre_build.sh
./scripts/codebuild/build.sh
./scripts/codebuild/post_build.sh

aws cloudformation deploy --template-file output.yml --stack-name $stackname \
  --capabilities CAPABILITY_NAMED_IAM \
  --parameter-overrides AppConfigPath="/all/$stackname" ContainerImageUrl="333680067100.dkr.ecr.us-east-1.amazonaws.com/marbl-image-1cs1q74l7njir:latest"

rm -rf output.yml
