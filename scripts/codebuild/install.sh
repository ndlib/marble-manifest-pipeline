#!/bin/bash
echo "[Install phase] `date` in `pwd`"

# copy the cloudformation into the project
cp $BLUEPRINTS_DIR/deploy/cloudformation/manifest-pipeline.yml ./

# install dependencies for specific lambdas
pushd find_objects
./local_install.sh
popd

pushd find_images_for_objects
./local_install.sh
popd

pushd send_objects_to_pipeline
./local_install.sh
popd

pushd process_manifest
./local_install.sh
popd

pushd init
./local_install.sh
popd
