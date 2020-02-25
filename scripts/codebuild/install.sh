#!/bin/bash
echo "[Install phase] `date` in `pwd`"

# copy the cloudformation into the project
cp $BLUEPRINTS_DIR/deploy/cloudformation/manifest-pipeline.yml ./

# install dependencies for specific lambdas
pushd send_objects_to_pipeline
./local_install.sh
popd

pushd process_manifest
./local_install.sh
popd

pushd init
./local_install.sh
popd

pushd finalize
./local_install.sh
popd

pushd museum_export
./local_install.sh
popd

pushd harvest_eads
./local_install.sh
popd

pushd aleph_export
./local_install.sh
popd
