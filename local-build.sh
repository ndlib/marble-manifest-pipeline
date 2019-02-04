PROGNAME=$0

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m' # No Color

usage() {
  cat << EOF >&2
Usage: $PROGNAME stage

deployes the pipeline to the stage

EOF
  exit 1
}

stage=$1


export S3_BUCKET=mellon-manifest-pipeline-dev2-s3bucket-1urf7wn3aoy37

./scripts/codebuild/install.sh
./scripts/codebuild/pre_build.sh
./scripts/codebuild/build.sh
./scripts/codebuild/post_build.sh

aws cloudformation deploy --template-file output.yml --stack-name mellon-manifest-pipeline-$stage
