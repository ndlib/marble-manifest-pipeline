# mellon-manifest-pipeline

This repo contains code used to harvest metadata from source systems and load that into dynamoDB.  This code also finds files (images or media files) that will need to be processed, and loads references to those into dynamoDB.

# Local Development
## Prerequisites
For consistent coding standards install [flake8](http://flake8.pycqa.org/en/latest/index.html) via [pip](https://pypi.org/project/pip/)

`pip install -r dev-requirements.txt`

To run [flake8](http://flake8.pycqa.org/en/latest/index.html) manually

`flake8 json-from-csv/handler.py`

The projects custom linter configurations can be found in .flake8

Additional options can be found [here](http://flake8.pycqa.org/en/latest/user/options.html)

Various Editors and IDEs have plugins that work with this linter.
 * In ATOM install [linter-flake8](https://atom.io/packages/linter-flake8)
 * In Sublime install [SublimeLinter-flake8](https://github.com/SublimeLinter/SublimeLinter-flake8)
 * In VS Code modify these [config settings](https://code.visualstudio.com/docs/python/settings-reference#_flake8)

## Deployment
Run tests before deploying by first connecting to aws-vault to establish a connection to aws, and then run the script called "run_all_tests.sh"

```bash
aws-vault exec testlibnd-superAdmin
./run_all_tests.sh
```

Run the script local-deploy providing the name of the stack you want to deploy and the
path to the marble-blueprints repo

```bash
./local-deploy.sh manifest-pipeline-jon ../marble-blueprints/
```

