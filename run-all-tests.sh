if [ -z "$1" ]
  then
    pip uninstall pipelineutilities

    ./scripts/codebuild/install.sh  ||  { exit 1; }
fi

./scripts/codebuild/pre_build.sh ||  { exit 1; }
