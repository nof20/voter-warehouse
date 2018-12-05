"""Scrape and analyze Legislation data.

"""

from __future__ import absolute_import

import argparse
import re
import logging
import sys
import apache_beam as beam
import threading
import xml.etree.ElementTree as ET

from collections import deque
from datetime import date, datetime
from requests import get
from bs4 import BeautifulSoup, Comment
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_2) AppleWebKit/604.4.7 (KHTML, like Gecko) Version/11.0.2 Safari/604.4.7"

SCHEMA_FIELDS = [
    ('keyword', 'string', 'nullable'),
    ('title', 'string', 'nullable'),
    ('url', 'string', 'nullable'),
    ('pubdate', 'timestamp', 'nullable'),
    ('text', 'string', 'nullable'),
    ('entities', 'record', 'repeated', (
        ('name', 'string', 'nullable'),
        ('salience', 'float', 'nullable'),
        ('score', 'float', 'nullable'),
        ('magnitude', 'float', 'nullable'),
        ('wikipedia_url', 'string', 'nullable'))
    )
]

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

    return output

def get_bills_year(year, key):
    offset = 1
    more = True
    total = None
    bills = []
    logging.info("Getting bills for {}".format(year))
    while more:
        payload = {
            'key': key,
            'view': 'info',
            'limit': 100,
            'offset': offset}
        r = get(
            'http://legislation.nysenate.gov/api/3/bills/' +
            year,
            params=payload)
        r.raise_for_status()
        total = r.json()['total']
        for bill in r.json()['result']['items']:
            bills.append(bill)
        offset += r.json()['result']['size']
        more = False if offset >= total else True
        print(".", end="")
    return bills

def get_bill_detail(year, printNo, key):
    payload = {'key': key, 'view': 'default'}
    r = get("http://legislation.nysenate.gov/api/3/bills/{}/{}".format(year,
                                                                           printNo), params=payload)
    r.raise_for_status()
    doc = r.json()['result']
    doc['year'] = year
    return doc



def format_bq(row):
    # Truncate and encode text to 64 kb UTF-8
    LIMIT = 64 * 1024
    row['text'] = row['text'][:LIMIT].encode('ascii', errors='replace')
    row['title'] = row['title'][:LIMIT].encode('ascii', errors='replace')
    row['url'] = row['url'][:LIMIT].encode('ascii', errors='replace')
    # Parse and re-encode pubdate timestamp
    pubdate = datetime.strptime(row['pubdate'], "%a, %d %b %Y %H:%M:%S %Z").isoformat()
    row['pubdate'] = pubdate
    return row

def run(argv=None):
    """Main entry point; defines and runs the pipeline."""
    logging.info("Starting pipeline.")

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--project=voterdb-test',
        '--job_name=news-pipeline',
        '--temp_location gs://voterdb-test-dataflow-temp/',
        '--staging_location gs://voterdb-test-dataflow-staging/',
        '--requirements_file=requirements.txt',
        '--max_num_workers=8',
        '--disk_size_gb=100'])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:

        # Creating an initial PCollection of one value is necessary to ensure
        # the HTTP gets are deferred until the DataflowRunner starts
        # in the cloud.  beam.Create(read_zip_csv()) otherwise creates a pickled
        # Python image which is bigger than the upload limit, and fails.

        raw = (p
            | "beam.Create" >> beam.Create(KEYWORDS)
            | "get_news_items" >> beam.FlatMap(get_news_items)
            | "BatchElements" >> beam.BatchElements()
            | "BatchRunner analyze" >> beam.ParDo(BatchRunner(), analyze)
            | "format_bq" >> beam.Map(format_bq)
            | "News.Semantic" >> beam.io.WriteToBigQuery(
                table='News.Semantic',
                schema=gen_schema(SCHEMA_FIELDS),
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
