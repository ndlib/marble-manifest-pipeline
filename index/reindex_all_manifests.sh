#!/bin/sh
# run_all.sh
# aws sts get-caller-identity  --profile libnd-dlt-admin; export AWS_PROFILE=libnd-dlt-admin
# aws s3 ls s3://mellon-manifest-prod-processbucket-1mn0hymubb5v9/finished/ --profile libnd-dlt-admin

# Following bucket is for manifests
BUCKET='s3://mellon-manifest-prod-processbucket-1mn0hymubb5v9/finished/'

# Following bucket is for collections
# BUCKET='s3://mellon-manifest-prod-manifestbucket-1aed76g9eb0if/collection/'

MANIFESTS=""
for path in $(aws s3 ls $BUCKET --profile libnd-dlt-admin); do
	path=${path%PRE} # strip PRE
	path=${path%/} # strip trailing slash
	if [ "$path" != "" ]
		then
			if [ "$MANIFESTS" != "" ]
				then
					MANIFESTS="$MANIFESTS,"
			fi
		MANIFESTS="$MANIFESTS \"$path\""
	fi
	# python -c "from make_manifest_discoverable import make_manifest_discoverable; make_manifest_discoverable('${path%/}')"
	# ${path%/} captures the relative path and removes the trailing slash
	done
# echo {\"ids\": [$MANIFESTS]} > collections.json
echo {\"ids\": [$MANIFESTS]} > manifests.json
