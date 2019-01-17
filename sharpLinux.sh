#!/bin/bash

echo "Installing packages"
cd pyramid-generator
npm install

echo "Installing LINUX sharp/libvips packages"
export SHARP_IGNORE_GLOBAL_LIBVIPS=true
rm -rf node_modules/sharp
npm install --arch=x64 --platform=linux --target=8.10.0 sharp