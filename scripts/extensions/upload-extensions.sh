usage() {
  cat << EOF >&2
Usage: $PROGNAME MANIFEST_BUCKET

Uploads the extensions to the prezi api
Note: Ensure you have an env variable MANIFEST_BUCKET as the bucket to copy to

EOF
  exit 1
}
``
MANIFEST_BUCKET=$1

if [ -z ${MANIFEST_BUCKET+x} ]; then
  usage
  exit 1
fi

aws s3 sync ./extensions s3://$MANIFEST_BUCKET/extensions/
