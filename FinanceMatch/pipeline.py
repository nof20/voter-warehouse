"""Match finance records with voter records.
"""

from __future__ import absolute_import

import argparse
import logging
import re
import sys
import apache_beam as beam
from pyusps import address_information


from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions
from datetime import date, datetime
from fuzzywuzzy.process import fuzz

SCHEMA_FIELDS = [
    ('SBOEID', 'string', 'nullable'),
    ('COUNTYVRNUMBER', 'string', 'nullable'),
    ('FilingYear', 'integer', 'nullable'),
    ('ContributorName', 'string', 'nullable'),
    ('ContributorAddr1', 'string', 'nullable'),
    ('ContributorAddr2', 'string', 'nullable'),
    ('FilerID', 'string', 'nullable'),
    ('FilerName', 'string', 'nullable'),
    ('FilerCommittee', 'string', 'nullable'),
    ('Office', 'string', 'nullable'),
    ('TotalDonation', 'float', 'nullable'),
    ('voter_match_string', 'string', 'nullable'),
    ('donor_match_string', 'string', 'nullable'),
    ('ratio', 'integer', 'nullable')
]

FINANCE_QUERY = """
SELECT FilingYear, ContributorName, ContributorAddr1, ContributorAddr2,
    FilerID, FilerName, FilerCommittee, Office, SUM(Amt) AS TotalDonation
FROM Finance.State
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
ORDER BY 1, 2, 3, 4, 5, 6, 7, 8
"""

VOTER_QUERY = """
SELECT *
FROM Voter.Raw
WHERE SD = 31
AND STATUS = "ACTIVE"
"""

INITIAL_LENGTH = 8
FUZZ_THRESHOLD = 70


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
                    output[name] = row[name].encode(
                        'utf-8', errors='ignore').strip()
                except Exception:
                    output[name] = None
            else:
                output[name] = None
        elif typ == 'integer':
            try:
                output[name] = int(str(row[name]).strip())
            except Exception as e:
                logging.error(
                    "Caught exception casting to int: {}".format(
                        str(e)))
                output[name] = None
        elif typ == 'float':
            try:
                output[name] = float(str(row[name]).strip())
            except Exception as e:
                logging.error(
                    "Caught exception casting to float: {}".format(
                        str(e)))
                output[name] = None

    logging.info("Returning: {}".format(str(output)))
    return output


class CachingFuzzer(object):

    def __init__(self, a):
        self.cache, self.a = {}, a

    def fuzz(self, b):
        if b not in self.cache:
            self.cache[b] = fuzz.ratio(self.a, b)
        return self.cache[b]

def filter_universe(x, match_string, length):
    if x is not None:
        if 'match_string' in x:
            if x['match_string'][:length] == match_string[:length]:
                return True

    return False

def get_voter(donor, dictlist):
    """Add voter data to donor data, if there's a match."""

    length = INITIAL_LENGTH
    donor_match_string = u"{}, {}, {}".format(
        donor['ContributorName'],
        donor['ContributorAddr1'],
        donor['ContributorAddr2']
    )
    logging.info(u"Starting get_voter for donor {}, donor_match_string {}".format(str(donor), donor_match_string))

    c = CachingFuzzer(donor_match_string)
    match = False
    while not match:
        universe = filter(
            lambda x: filter_universe(x, donor_match_string, length), dictlist)
        if len(universe):
            filtered_universe = []
            for u in universe:
                u['ratio'] = c.fuzz(u['match_string'])
                if u['ratio'] >= FUZZ_THRESHOLD:
                    filtered_universe.append(u)
            if filtered_universe:
                sorted_universe = sorted(
                    filtered_universe,
                    key=lambda x: x['ratio'],
                    reverse=True)
                top_all = sorted_universe[0]
                top = {k: top_all[k]
                       for k in ['ratio', 'SBOEID', 'COUNTYVRNUMBER', 'match_string']}
                logging.info(
                    u"Match: {} matches {}".format(
                        donor_match_string, top))
                match = True
                dct = dict(donor).copy()
                logging.info(u"Adding data to donor: {}".format(str(donor)))
                dct.update(top)
                dct['voter_match_string'] = top['match_string']
                dct['donor_match_string'] = donor_match_string
                logging.info(u"About to save: {}".format(str(dct)))
                return dct
        if length > 1:
            length -= 1
        else:
            # Give up
            match = True
            return donor


def standardize_address(batch, usps_key):
    # Form voter addresses
    post_data = []
    if batch is None:
        return []

    for row in batch:
        try:
            raddnumber = row['RADDNUMBER'].strip() if row['RADDNUMBER'] else ""
            rstreetname = row['RSTREETNAME'].strip(
            ) if row['RSTREETNAME'] else ""
            if row['RAPARTMENT']:
                if ('APT' in row['RAPARTMENT']) or (
                        'UNIT' in row['RAPARTMENT']) or ('PH' in row['RAPARTMENT']):
                    rapartment = row['RAPARTMENT']
                else:
                    rapartment = "APT {}".format(row['RAPARTMENT'])
            else:
                rapartment = ""
            post_data.append({
                'address': u" ".join([raddnumber, rstreetname, rapartment]),
                'city': row['RCITY'],
                'state': 'NY'
            })
        except Exception as e:
            logging.Info(
                "Could not form address in standardize_address, error: {}".format(e))
            post_data.append(None)

    # Submit batch to API
    try:
        recv_data = address_information.verify(usps_key, *post_data)
    except Exception as e:
        logging.error("Caught exception posting to standardize_address: {}".format(e))

    # Match
    output = []
    for i, row in enumerate(batch):
        if row is not None:
            out_dct = row.copy()
        else:
            continue

        # Try and use formatted address
        try:
            out_dct['voter_addr1'] = recv_data[i]['address']
            if isinstance(recv_data[i]['zip5'], int):
                # So defensive
                recv_data[i]['zip5'] = "{:0.0f}".format(recv_data[i]['zip5'])
            out_dct['voter_addr2'] = "{}, {} {}".format(
                recv_data[i]['city'], recv_data[i]['state'], recv_data[i]['zip5'])
        except Exception as e:
            # Output from pyusps is Exception not dict, etc.; fall back on
            # constructed string
            try:
                out_dct['voter_addr1'] = post_data[i]['address']
            except Exception as e:
                # e.g. because post_data[i] is None
                out_dct['voter_addr1'] = None
            out_dct['voter_addr2'] = "{}, NY {}".format(
                row['RCITY'], row['RZIP5'])

        # Form match string, whatever happens
        out_dct['match_string'] = "{} {}, {}, {}".format(
            out_dct['LASTNAME'].strip(),
            out_dct['FIRSTNAME'].strip(),
            out_dct['voter_addr1'],
            out_dct['voter_addr2']
        )
        output.append(out_dct)
    return output


def run(argv=None):
    """Main entry point; defines and runs the pipeline."""
    logging.info("Starting pipeline.")

    parser = argparse.ArgumentParser()
    parser.add_argument('--usps_key',
                        dest='usps_key',
                        default=None,
                        help='USPS API key')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--project=voterdb-test',
        '--job_name=financematch-pipeline',
        '--temp_location gs://voterdb-test-dataflow-temp/',
        '--staging_location gs://voterdb-test-dataflow-staging/',
        '--max_num_workers=32',
        '--disk_size_gb=50'])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:

        voter = (p
            | "VOTER_QUERY" >> beam.io.Read(
                beam.io.BigQuerySource(query=VOTER_QUERY))
            | "BatchElements" >> beam.BatchElements(max_batch_size=5)
            | "standardize_address" >> beam.FlatMap(standardize_address,known_args.usps_key))

        out = (p
            | "FINANCE_QUERY" >> beam.io.Read(
                   beam.io.BigQuerySource(query=FINANCE_QUERY))
            | "get_voter" >> beam.Map(get_voter, beam.pvalue.AsList(voter))
            | "flatten_format" >> beam.Map(flatten_format)
            | "Finance.Match" >> beam.io.WriteToBigQuery(
                   table='Finance.Match',
                   schema=gen_schema(SCHEMA_FIELDS),
                   write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                   create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
