#!/bin/bash
echo "[Install phase] `date` in `pwd`"

# install dependencies for specific lambdas

pushd aleph_export
./local_install.sh
popd

pushd archivesspace_export
./local_install.sh
popd

pushd bendo_export
./local_install.sh
popd

pushd clean_up_dynamo
./local_install.sh
popd

pushd copy_media_content
./local_install.sh
popd

pushd curate_export
./local_install.sh
popd

pushd expand_subject_terms_lambda
./local_install.sh
popd

pushd metadata_rules
./local_install.sh
popd

pushd museum_export
./local_install.sh
popd

pushd object_files_api
./local_install.sh
popd
