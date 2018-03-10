"""Match finance records with voter records.
"""

from __future__ import absolute_import

import argparse
import logging
import re
import sys
import apache_beam as beam

from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import date, datetime
from fuzzywuzzy.process import fuzz

SCHEMA_FIELDS = [
    ('COUNTYVRNUMBER', 'string', 'nullable'),
    ('Contributor', 'string', 'nullable'),
    ('FilerName', 'string', 'nullable'),
    ('FilingYear', 'integer', 'nullable'),
    ('SBOEID', 'string', 'nullable'),
    ('TotalDonation', 'float', 'nullable'),
    ('match_string', 'string', 'nullable'),
    ('ratio', 'integer', 'nullable')
]

FINANCE_QUERY = """
SELECT FilingYear, FilerName, Contributor, SUM(Amt) AS TotalDonation
FROM Finance.State
WHERE Office = "State Senator"
AND Dist = 31
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
"""

VOTER_QUERY = """
SELECT *
FROM Voter.Raw
WHERE SD = 31
AND STATUS = "ACTIVE"
"""

INITIAL_LENGTH = 8
FUZZ_THRESHOLD = 88

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
    logging.info("Starting flatten_format: {}".format(str(row)))
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
                output[name] = int(str(row[name]).strip())
            except Exception as e:
                logging.error("Caught exception casting to int: {}".format(str(e)))
                output[name] = None
        elif typ == 'float':
            try:
                output[name] = float(str(row[name]).strip())
            except Exception as e:
                logging.error("Caught exception casting to float: {}".format(str(e)))
                output[name] = None

    logging.info("Returning: {}".format(str(output)))
    return output

def enhance_voter(row):
    address = "{} {} {}\n{}, NY {}".format(
        str(row['RADDNUMBER']).strip() if 'RADDNUMBER' in row else "",
        str(row['RSTREETNAME']).strip() if "RSTREETNAME" in row else "",
        "APT {}".format(
            str(row['RAPARTMENT']).strip()) if 'RAPARTMENT' in row else "",
        str(row['TOWNCITY']).strip()\
            .replace('MANHATTAN', 'NEW YORK')\
            .replace('BRONX', 'NEW YORK') if 'TOWNCITY' in row else "",
        str(row['RZIP5']).strip() if 'RZIP5' in row else "")
    row['match_string'] = "{}, {} {}\n{}".format(
        str(row['LASTNAME']).strip(),
        str(row['FIRSTNAME']).strip(),
        str(row['MIDDLENAME'])[0] if row['MIDDLENAME'] else "",
        address).upper().replace("  ", " ").replace("APT APT", "APT").replace(".0", "")
    return row

class CachingFuzzer(object):

    def __init__(self, a):
        self.cache, self.a = {}, a

    def fuzz(self, b):
        if b not in self.cache:
            self.cache[b] = fuzz.ratio(self.a, b)
        return self.cache[b]

def get_voter(donor, dictlist):
    """Add voter data to donor data, if there's a match."""

    length = INITIAL_LENGTH
    c = CachingFuzzer(donor['Contributor'])
    match = False
    while not match:
        universe = filter(lambda x: x['match_string'][:length] == donor['Contributor'][:length],\
                          dictlist)
        if len(universe):
            filtered_universe = []
            for u in universe:
                u['ratio'] = c.fuzz(u['match_string'])
                if u['ratio'] >= FUZZ_THRESHOLD:
                    filtered_universe.append(u)
            if filtered_universe:
                sorted_universe = sorted(filtered_universe, key=lambda x: x['ratio'], reverse=True)
                top_all = sorted_universe[0]
                top = {k: top_all[k] for k in ['match_string', 'ratio', 'SBOEID', 'COUNTYVRNUMBER']}
                logging.info("Match: {} matches {}".format(donor['Contributor'], top))
                match = True
                dct = dict(donor).copy()
                logging.info("Adding data to donor: {}".format(str(donor)))
                dct.update(top)
                logging.info("About to save: {}".format(str(dct)))
                return dct
        if length > 1:
            length -= 1
        else:
            # Give up
            match = True
            return donor

def run(argv=None):
    """Main entry point; defines and runs the pipeline."""
    logging.info("Starting pipeline.")

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--project=voterdb-test',
        '--job_name=financematch-pipeline',
        '--temp_location gs://voterdb-test-dataflow-temp/',
        '--staging_location gs://voterdb-test-dataflow-staging/',
        '--max_num_workers=8',
        '--disk_size_gb=50'])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:

        voter = (p
            | "VOTER_QUERY" >> beam.io.Read(
                beam.io.BigQuerySource(query=VOTER_QUERY))
            | "enhance_voter" >> beam.Map(enhance_voter))

        out = (p
            | "FINANCE_QUERY" >> beam.io.Read(
                beam.io.BigQuerySource(query=FINANCE_QUERY))
            | "get_voter" >> beam.Map(get_voter,
                beam.pvalue.AsList(voter))
            | "flatten_format" >> beam.Map(flatten_format)
            | "Finance.Match" >> beam.io.WriteToBigQuery(
                table='Finance.Match',
                schema=gen_schema(SCHEMA_FIELDS),
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
