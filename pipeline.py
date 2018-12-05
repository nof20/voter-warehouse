"""The voter-warehouse Dataflow pipeline wrapper.
"""

from __future__ import absolute_import

import argparse
import httplib2
import json
import logging
import re

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

from voter_warehouse import Census

KNOWN_MODULES = {
    'census': Census.main
}

def run(argv=None):
  parser = argparse.ArgumentParser()
  parser.add_argument('--module',
                      required=True,
                      help='Module to execute')
  known_args, pipeline_args = parser.parse_known_args(argv)

  pipeline_args.extend([
      '--project=voterdb-test',
      '--staging_location=gs://voterdb-test-dataflow/',
      '--temp_location=gs://voterdb-test-dataflow/',
      '--job_name=voter-warehouse-{}'.format(known_args.module),
      '--setup_file=./setup.py',
      '--max_num_workers=32',
      '--disk_size_gb=50'
  ])

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:
      if known_args.module in KNOWN_MODULES:
          KNOWN_MODULES[known_args.module](p)
      else:
          raise Exception("Module not known.")

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
