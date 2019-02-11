#!/bin/bash
echo "[Install phase] `date` in `pwd`"

echo "Installing LINUX sharp/libvips packages"
cd pyramid-generator
npm install --only=prod

export SHARP_IGNORE_GLOBAL_LIBVIPS=true
rm -rf node_modules/sharp
npm install --arch=x64 --platform=linux --target=8.10.0 sharp

# fixes incorrect created files from the extract.
find ./node_modules -mtime +10950 -exec touch {} \;

# copy the cloudformation into the project
cp $BLUEPRINTS_DIR/deploy/cloudformation/manifest-pipeline.yml ./
