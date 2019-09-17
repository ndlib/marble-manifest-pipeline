#!/bin/bash
echo "[Install phase] `date` in `pwd`"

# copy the cloudformation into the project
cp $BLUEPRINTS_DIR/deploy/cloudformation/manifest-pipeline.yml ./
