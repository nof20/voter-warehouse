"""Read REDIST_EQUIV data from LATFOR."""

import requests
import zipfile
import io
import csv
import logging
import sys

from google.cloud import bigquery
from google.cloud.exceptions import NotFound

URL = 'http://www.latfor.state.ny.us/data/2010equiv/REDIST_EQUIV.zip'

SCHEMA = [
    bigquery.SchemaField('YR', 'INTEGER'),
    bigquery.SchemaField('STATE', 'INTEGER'),
    bigquery.SchemaField('COUNTY', 'INTEGER'),
    bigquery.SchemaField('COUSUB', 'INTEGER'),
    bigquery.SchemaField('AD', 'INTEGER'),
    bigquery.SchemaField('ED', 'INTEGER'),
    bigquery.SchemaField('VTD08', 'INTEGER')
]

def run():
    # Get data from LATFOR website
    resp = requests.get(URL)
    resp.raise_for_status()
    logging.info("Got data from {}, size {} kb".format(URL,
        sys.getsizeof(resp.content)/1024))
    b = io.BytesIO(resp.content)
    zf = zipfile.ZipFile(b)
    rows = []
    for filename in zf.namelist():
        if '.csv' in filename:
            fh = zf.open(filename)
            reader = csv.DictReader(fh)
            rows.extend([r for r in reader])

    # Create table in BQ
    client = bigquery.Client()
    dataset_ref = client.dataset('Census')
    dataset = bigquery.Dataset(dataset_ref)
    table_ref = dataset.table('RedistEquiv')
    try:
        client.delete_table(table_ref)
        logging.info("Deleted old table, creating new one")
    except NotFound:
        # Ignore
        pass

    table = bigquery.Table(table_ref, schema=SCHEMA)
    table = client.create_table(table)

    # Save data to BQ
    logging.info("Streaming rows to BigQuery")
    rows_finished = 0
    while rows_finished < len(rows):
        sending = rows[rows_finished:(rows_finished + 5000)]
        errors = client.create_rows(table, sending)
        if errors:
            logging.error("Caught error: {}".format(errors))
            sys.exit()
        rows_finished += len(sending)

if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)
    run()
