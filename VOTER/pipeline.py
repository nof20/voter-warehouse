"""A pipeline to process New York State voter file and store in BigQuery.
"""

from __future__ import absolute_import

import argparse
import logging
import csv

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

RAW_VF_SCHEMA = (
     "LASTNAME:STRING,FIRSTNAME:STRING,MIDDLENAME:STRING,"
     "NAMESUFFIX:STRING,RADDNUMBER:STRING,RHALFCODE:STRING,"
     "RAPARTMENT:STRING,RPREDIRECTION:STRING,RSTREETNAME:STRING,"
     "RPOSTDIRECTION:STRING,RCITY:STRING,RZIP5:STRING,RZIP4:STRING,"
     "MAILADD1:STRING,MAILADD2:STRING,MAILADD3:STRING,MAILADD4:STRING,"
     "DOB:STRING,GENDER:STRING,ENROLLMENT:STRING,OTHERPARTY:STRING,"
     "COUNTYCODE:INTEGER,ED:INTEGER,LD:INTEGER,TOWNCITY:STRING,"
     "WARD:STRING,CD:INTEGER,SD:INTEGER,AD:INTEGER,LASTVOTEDDATE:STRING,"
     "PREVYEARVOTED:STRING,PREVCOUNTY:STRING,PREVADDRESS:STRING,"
     "PREVNAME:STRING,COUNTYVRNUMBER:STRING,REGDATE:STRING,VRSOURCE:STRING,"
     "IDREQUIRED:STRING,IDMET:STRING,STATUS:STRING,REASONCODE:STRING,"
     "INACT_DATE:STRING,PURGE_DATE:STRING,SBOEID:STRING,"
     "VoterHistory:STRING,Phone:STRING")

class DictFromRawLine(beam.DoFn):
    """Create Dict of strings from raw line of Voter File CSV."""

    def process(self, element):
        try:
            for data in csv.reader([element]):
                dct = {}
                for i, field in enumerate(RAW_VF_SCHEMA.split(",")):
                    name = field.split(":")[0]
                    try:
                        dct[name] = data[i].encode('utf-8').strip()
                    except Exception:
                        dct[name] = None
                yield dct
        except Exception:
            pass

def ConvertTypes(element):
    INT_FIELDS = ['COUNTYCODE', 'ED', 'LD', 'CD', 'SD', 'AD']
    for field in INT_FIELDS:
        try:
            element[field] = int(element[field])
        except ValueError:
            element[field] = None
    return element

def run(argv=None):
  """Main entry point; defines and runs the pipeline."""
  logging.info("Starting pipeline.")

  parser = argparse.ArgumentParser()
  known_args, pipeline_args = parser.parse_known_args(argv)
  pipeline_args.extend([
    '--runner=DataflowRunner',
    '--project=voterdb-test',
    '--job_name=voter-pipeline'])

  pipeline_options = PipelineOptions(pipeline_args)
  pipeline_options.view_as(SetupOptions).save_main_session = True
  with beam.Pipeline(options=pipeline_options) as p:

     # TODO: Select rather than hard-code bucket/file name
    raw = (p
        | "beam.io.ReadFromText" >> beam.io.ReadFromText("gs://upload-raw/AllNYSVoters_2017-12-27.csv")
        | "DictFromRawLine" >> beam.ParDo(DictFromRawLine()))

    output = (raw
        | "ConvertTypes" >> beam.Map(lambda x: ConvertTypes(x))
        | "WriteRaw" >> beam.io.Write(
            beam.io.BigQuerySink(table='VOTER.VoterFileRaw',
                validate=True,
                schema=RAW_VF_SCHEMA)))

    output = (raw
        | "FilterDict" >> beam.Map(lambda x: {'SBOEID': x['SBOEID'], 'STATUS': x['STATUS']})
        | "WriteFiltered" >> beam.io.Write(
            beam.io.BigQuerySink(table='VOTER.FilterTest',
                validate=True,
                schema='SBOEID:STRING,STATUS:STRING')))

    #output | beam.io.WriteToText('gs://output-stats/addresses.txt')

if __name__ == '__main__':
  logging.getLogger().setLevel(logging.INFO)
  run()
