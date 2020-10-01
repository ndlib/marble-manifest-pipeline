from setuptools import setup, find_packages


setup(name='pipelineutilities',
      version='1.0',
      packages=find_packages(),
      install_requires=['jsonschema', 'requests'],
      # package_dir={'': '.'},
      # data_files=[('.', ['pipelineutilities/language_codes.json', 'pipelineutilities/source_system_export_ids.json'])]
      )
