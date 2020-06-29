#!/bin/bash
echo "[Install phase] `date` in `pwd`"

# copy the cloudformation into the project
cp $BLUEPRINTS_DIR/deploy/cloudformation/manifest-pipeline.yml ./

# install dependencies for specific lambdas
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

pushd curate_export
./local_install.sh
popd

pushd archivesspace_export
./local_install.sh
popd

pushd bendo_export
./local_install.sh
popd

pushd metadata_rules
./local_install.sh
popd
