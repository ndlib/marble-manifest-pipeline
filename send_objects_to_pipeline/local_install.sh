#!/bin/bash
# This script assumes we are starting in the application root folder
# install dependencies
magenta=`tput setaf 5`
reset=`tput sgr0`

echo "\n\n ${magenta}----- send_objects_to_pipeline LOCAL_INSTALL.SH -----${reset}"

# install dependencies in dependencies folder that will need to be included with deployed lambda
mkdir dependencies

# install dependencies into ./dependencies folder
pip install -r requirements.txt -t ./dependencies
