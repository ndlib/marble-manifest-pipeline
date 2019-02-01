#!/bin/bash
echo "[Install phase] `date` in `pwd`"

cd pyramid-generator
npm install --only=prod

echo "Installing LINUX sharp/libvips packages"
export SHARP_IGNORE_GLOBAL_LIBVIPS=true
rm -rf node_modules/sharp
npm install --arch=x64 --platform=linux --target=8.10.0 sharp
