export S3_BUCKET=mellon-manifest-pipeline-dev2-s3bucket-1urf7wn3aoy37

./scripts/codebuild/install.sh
./scripts/codebuild/pre_build.sh
./scripts/codebuild/build.sh
./scripts/codebuild/post_build.sh
