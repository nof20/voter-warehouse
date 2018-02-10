"""Process data from OpenElections.

"""

from __future__ import absolute_import

import argparse
import logging
import csv
import requests
import zipfile
import io
import logging
import sys
import re

from datetime import date, datetime
from apache_beam.io.gcp.internal.clients import bigquery

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


SCHEMA_FIELDS = [
    ('election_date', 'date', 'nullable'),
    ('state', 'string', 'nullable'),
    ('scope', 'string', 'nullable'),
    ('county', 'string', 'nullable'),
    ('precinct', 'string', 'nullable'),
    ('office', 'string', 'nullable'),
    ('district', 'string', 'nullable'),
    ('candidate', 'string', 'nullable'),
    ('party', 'string', 'nullable'),
    ('votes', 'integer', 'nullable'),
    ('absentee_military_votes', 'integer', 'nullable'),
    ('affidavit_votes', 'integer', 'nullable'),
    ('emergency_votes', 'integer', 'nullable'),
    ('federal_votes', 'integer', 'nullable'),
    ('manually_counted_emergency', 'integer', 'nullable'),
    ('public_counter_votes', 'string', 'nullable'),
    ('special_presidential', 'integer', 'nullable'),
    ('filename', 'string', 'required'),
    ('update_timestamp', 'timestamp', 'required')
]

OE_URLS = ['https://github.com/openelections/openelections-data-ny/archive/master.zip']
BLACKLIST = ['openelections-data-ny-master/county_matrix.csv']

class ReadZipCsv(beam.DoFn):

    def process(self, URL, blacklist):
        resp = requests.get(URL)
        now = datetime.now().isoformat()
        resp.raise_for_status()
        logging.info("Got data from {}, size {:0,.0f} kb".format(URL,
            sys.getsizeof(resp.content)/1024))
        b = io.BytesIO(resp.content)
        zf = zipfile.ZipFile(b)
        for filename in zf.namelist():
            if ('.csv' in filename) and (filename not in BLACKLIST):
                with zf.open(filename, 'rU') as fh:
                    reader = csv.DictReader(fh)
                    for r in reader:
                        row = r.copy()
                        row['filename'] = filename
                        row['update_timestamp'] = now
                        yield row
        del(resp)

def gen_kind_schema(tup):
    kind_schema = bigquery.TableFieldSchema()
    kind_schema.name = tup[0]
    kind_schema.type = tup[1]
    kind_schema.mode = tup[2]
    if kind_schema.type == 'record':
        for nest_tup in tup[3]:
            nest_schema = gen_kind_schema(nest_tup)
            kind_schema.fields.append(nest_schema)
    return kind_schema

def gen_schema(fields):
    """See https://beam.apache.org/documentation/sdks/pydoc/0.6.0/_modules/apache_beam/examples/cookbook/bigquery_schema.html."""

    table_schema = bigquery.TableSchema()
    for tup in fields:
        kind_schema = gen_kind_schema(tup)
        table_schema.fields.append(kind_schema)
    return table_schema

def flatten_format(row):
    # Map mis-spelled columns
    cols = {'absentee_military_votes': ['absentee', 'absentee_hc'],
        'affidavit_votes': ['affidavit'],
        'district': ['disctrict']}
    for k, vv in cols.items():
        for v in vv:
            if v in row:
                row[k] = row[v]

    # Map and type-convert
    output = {}
    for name, typ, mode in SCHEMA_FIELDS:
        if typ in ['string', 'timestamp', 'date']:
            if name in row:
                try:
                    output[name] = row[name].encode('utf-8', errors='ignore').strip()
                except Exception:
                    output[name] = None
            else:
                output[name] = None
        elif typ == 'integer':
            try:
                output[name] = int(row[name].strip())
            except Exception:
                output[name] = None

    # Parse data from filename
    # e.g. 'openelections-data-ny-master/2000/20001107__ny__general.csv'
    m = re.match(r'.*/(\d{4})/(\d{4})(\d{2})(\d{2})__(\w{2})__(.*).csv',
        row['filename'])
    if m:
        output['election_date'] = str(date(int(m.group(2)), int(m.group(3)), int(m.group(4))))
        output['state'] = m.group(5).upper()
        output['scope'] = m.group(6)
    else:
        output['election_date'] = None
        output['state'] = None
        output['scope'] = None

    return output

def run(argv=None):
    """Main entry point; defines and runs the pipeline."""
    logging.info("Starting pipeline.")

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--project=voterdb-test',
        '--job_name=results-pipeline',
        '--temp_location gs://voterdb-test-dataflow-temp/',
        '--staging_location gs://voterdb-test-dataflow-staging/',
        '--max_num_workers=8',
        '--disk_size_gb=100'])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:

        # Creating an initial PCollection of one value is necessary to ensure
        # the HTTP get to Github is deferred until the DataflowRunner starts
        # in the cloud.  beam.Create(read_zip_csv()) creates a pickled
        # Python image which is bigger than the upload limit, and fails.

        raw = (p
            | "beam.Create" >> beam.Create(OE_URLS)
            | "ReadZipCsv" >> beam.ParDo(ReadZipCsv(), BLACKLIST))

        output = (raw
            | "flatten_format" >> beam.Map(flatten_format)
            | "Results.OpenElections" >> beam.io.WriteToBigQuery(
                table='Results.OpenElections',
                schema=gen_schema(SCHEMA_FIELDS),
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
