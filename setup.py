from distutils.core import setup

setup(name='bq-dts-partner-sdk',
      version='0.0',
      description='BigQuery Data Transfer Service - Integration SDK',
      url='https://github.com/mtai/bq-dts-partner-sdk/',
      packages=['bq_dts'],
      package_data={'bq_dts': ['api_discovery.json']},
      data_files=[('bin', ['bin/data_source_definition.py'])],
      )
