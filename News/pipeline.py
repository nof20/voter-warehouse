"""Scrape and analyze news sentiment data.

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
from urllib import quote
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types
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

KEYWORDS = ['Lee Zeldin', 'Jeff Klein', 'John Faso', 'Marisol Alcantara',
    'David Valesky', 'David Carlucci', 'Diane Savino', 'Tony Avella',
    'Jose Peralta']

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

def tag_visible(element):
    if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
        return False
    if isinstance(element, Comment):
        return False
    return True

def text_from_html(body):
    soup = BeautifulSoup(body)
    texts = soup.findAll(text=True)
    visible_texts = filter(tag_visible, texts)
    return u" ".join(t.strip() for t in visible_texts)


def get_news_items(row):
    quoted = quote(row)
    url = 'https://news.google.com/news/rss/search/section/q/{}/{}'.format(quoted, quoted)
    params = {'ned': 'us', 'gl': 'US', 'hl': 'en'}
    resp = get(url, params=params, headers={'User-Agent': USER_AGENT})
    resp.raise_for_status()
    tree = ET.fromstring(resp.text.encode('ascii', errors='replace'))
    items = tree.find('channel').findall('item')
    output = []
    for item in items:
        dct = {item[i].tag: item[i].text for i in range(len(item))}
        text = None
        logging.info("Found keyword '{}' in URL '{}'".format(row, dct['link']))
        try:
            rr = get(url, headers={'User-Agent': USER_AGENT})
            rr.raise_for_status()
            soup = BeautifulSoup(rr.text)
            text = text_from_html(rr.text)
            if isinstance(text, str):
                text = unicode(text, errors='replace')
            if isinstance(dct['title'], str):
                dct['title'] = unicode(dct['title'], errors='replace')
            if isinstance(dct['link'], str):
                dct['link'] = unicode(dct['link'], errors='replace')
        except Exception as err:
            logging.error('Failed getting keyword "{}" in URL {}: {}'.format(row, url, str(err)))

        output.append({'keyword': row,
            'title': dct['title'],
            'url': dct['link'],
            'pubdate': dct['pubDate'],
            'text': text})
    if len(items) == 0:
        logging.error("Zero items in Google News feed, something went wrong")
    return output

class BatchRunner(beam.DoFn):

    def process(self, batch, fn):
        threads = deque()
        results = []
        for row in batch:
            if len(threads) > 30:
                t = threads.popleft()
                t.join()

            t = threading.Thread(target=fn,
                    args=(row, results))
            t.start()
            threads.append(t)

        while threads:
            t = threads.popleft()
            t.join()

        for r in results:
            yield r

def analyze(row, results):
    if row['text']:
        logging.info("Analyzing URL {}".format(row['url']))
        # See https://developers.google.com/api-client-library/python/guide/thread_safety.
        client = language.LanguageServiceClient()
        document = types.Document(
            content=row['text'],
            type=enums.Document.Type.PLAIN_TEXT)
        if sys.maxunicode == 65535:
            encoding = enums.EncodingType.UTF16
        else:
            encoding = enums.EncodingType.UTF32
        try:
            result = client.analyze_entity_sentiment(document, encoding)
            output = []
            for entity in result.entities:
                if entity.salience > 0.001:
                    if 'wikipedia_url' in entity.metadata:
                        wikipedia_url = entity.metadata['wikipedia_url']
                    else:
                        wikipedia_url = None
                    output.append({'name': entity.name,
                                   'salience': entity.salience,
                                   'score': entity.sentiment.score,
                                   'magnitude': entity.sentiment.magnitude,
                                   'wikipedia_url': wikipedia_url})
            row['entities'] = output
            results.append(row)
            return
        except Exception as err:
            logging.error('Exception in analysis: {}'.format(err))


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
        '--disk_size_gb=50'])

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
