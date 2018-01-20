"""A pipeline to process New York State voter file and store in BigQuery.

TODO:
- Move GCS input file to command-line
- Pick local/remote mode easier
- Split VoterHistory, although unfortunatley this requires more involved schema definitions:
    https://reformatcode.com/code/python/json-table-schema-to-bigquerytableschema-for-bigquerysink

USPS address validation is presently disabled in the code below.

"""

from __future__ import absolute_import

import argparse
import logging
import csv
import threading

#from pyusps import address_information
from datetime import date, datetime
from collections import deque

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
    "WARD:INTEGER,CD:INTEGER,SD:INTEGER,AD:INTEGER,LASTVOTEDDATE:STRING,"
    "PREVYEARVOTED:STRING,PREVCOUNTY:STRING,PREVADDRESS:STRING,"
    "PREVNAME:STRING,COUNTYVRNUMBER:STRING,REGDATE:STRING,VRSOURCE:STRING,"
    "IDREQUIRED:STRING,IDMET:STRING,STATUS:STRING,REASONCODE:STRING,"
    "INACT_DATE:STRING,PURGE_DATE:STRING,SBOEID:STRING,"
    "VoterHistory:STRING,Phone:STRING")

FORMATTED_SCHEMA = (
    "FullName:STRING,Gender:STRING,Enrollment:STRING,OtherPartyEnrollment:STRING,"
    "County:STRING,ElectionDistrict:INTEGER,LegislativeDistrict:INTEGER,TownCityCode:STRING,"
    "Ward:INTEGER,CongressionalDistrict:INTEGER,SenateDistrict:INTEGER,"
    "AssemblyDistrict:INTEGER,PreviousName:STRING,CountyVoterNumber:STRING,"
    "VoterRegistrationSource:STRING,IDRequired:STRING,IDMet:STRING,"
    "SBOEID:STRING,DateOfBirth:DATE,LastVotedDate:DATE,Age:INTEGER,"
    "Address:STRING,StreetAddress:STRING,Prime:BOOLEAN,FiveYearElections:INTEGER,FiveYearPrimaries:INTEGER")

COUNTS_SCHEMA = (
    "COUNTYCODE:INTEGER,County:STRING,AD:INTEGER,"
    "WARD:INTEGER,ED:INTEGER,ENROLLMENT:STRING,Count:INTEGER")

class DictFromRawLine(beam.DoFn):
    """Parse line in input CSV and generate row in VoterFileRaw table."""

    def process(self, element):
        try:
            for data in csv.reader([element]):
                dct = {}
                for i, field in enumerate(RAW_VF_SCHEMA.split(",")):
                    name, typ = field.split(":")
                    try:
                        if typ == "STRING":
                            dct[name] = data[i].encode('utf-8').strip()
                        elif typ == "INTEGER":
                            try:
                                dct[name] = int(data[i].encode('utf-8').strip())
                            except ValueError:
                                logging.info("Could not parse int in field {}".format(name))
                                dct[name] = None
                    except Exception as err:
                        logging.info("Error in field parsing: {}".format(str(err)))
                        dct[name] = None
                yield dct
        except Exception as err:
            logging.error("Error in csv.reader: {}".format(str(err)))


def vf_standardize_address(row, usps_key):
    """Used for the NY State Voter File only."""
    rhalfcode = '' if not row['RHALFCODE'] else row['RHALFCODE']
    raddnumber = '' if not row['RADDNUMBER'] else row['RADDNUMBER']
    rpredirection = '' if not row['RPREDIRECTION'] else row['RPREDIRECTION']
    rstreetname = '' if not row['RSTREETNAME'] else row['RSTREETNAME']
    rpostdirection = '' if not row['RPOSTDIRECTION'] else row['RPOSTDIRECTION']
    rapartment = '' if not row['RAPARTMENT'] else row['RAPARTMENT']

    # Build and verify full address, including apartment code
    if ('APT' in str(row['RAPARTMENT']).upper()) \
            or ('UNIT' in str(row['RAPARTMENT']).upper()) \
            or (row['RAPARTMENT'] == ''):
        address = "{} {} {} {} {} {}".format(
            raddnumber,
            rhalfcode,
            rpredirection,
            rstreetname,
            rpostdirection,
            rapartment).replace("  ", " ")
    else:
        address = "{} {} {} {} {} APT {}".format(
            raddnumber,
            rhalfcode,
            rpredirection,
            rstreetname,
            rpostdirection,
            rapartment).replace("  ", " ")
    try:
        address = address.upper()
        fmt_address = ", ".join(
            address,
            row['RCITY'],
            "NY {}".format(
                row['RZIP5']))
    except Exception:
        fmt_address = None

    # Build and verify street address (for geolocation)
    street = "{} {} {} {} {}".format(
        raddnumber,
        rhalfcode,
        rpredirection,
        rstreetname,
        rpostdirection)

    try:
        street = street.upper()
        fmt_street = ", ".join(
            street,
            row['RCITY'],
            "NY {}".format(
                row['RZIP5']))
    except Exception:
        fmt_street = None

    return fmt_address, fmt_street


def make_kv_pair(element, keys):
    """Convert a flat dict from BQ to a K, V pair for AsDict"""
    if ~isinstance(keys, list):
        """Keys is a single value."""
        el_keys = set(element.keys())
        el_keys.remove(keys)
        k = element[keys]
        v = {kk: element[kk] for kk in el_keys}

    else:
        """Keys is a list, so expect to index with a tuple."""
        el_keys = set(element.keys())
        for key in keys:
            el_keys.remove(key)
        k = tuple([element[kk] for kk in keys])
        v = {element[kk] for kk in el_keys}

    return k, v


# def build_formatted(element, usps_key, results):


def build_formatted(element, usps_key, elections, counties):
    """Generate row in formatted table."""

    # Ignore inactive
    if element['STATUS'] != 'ACTIVE':
        return []

    # Copy retained fields
    RETAIN = {'GENDER': 'Gender',
              'ENROLLMENT': 'Enrollment',
              'OTHERPARTY': 'OtherPartyEnrollment',
              'ED': 'ElectionDistrict',
              'LD': 'LegislativeDistrict',
              'TOWNCITY': 'TownCityCode',
              'WARD': 'Ward',
              'CD': 'CongressionalDistrict',
              'SD': 'SenateDistrict',
              'AD': 'AssemblyDistrict',
              'PREVNAME': 'PreviousName',
              'COUNTYVRNUMBER': 'CountyVoterNumber',
              'VRSOURCE': 'VoterRegistrationSource',
              'IDREQUIRED': 'IDRequired',
              'IDMET': 'IDMet',
              'SBOEID': 'SBOEID'}
    new = {RETAIN[k]: element[k] for k in RETAIN}

    # Name.
    new['FullName'] = "{} {} {} {}".format(
        element['FIRSTNAME'], element['MIDDLENAME'],
        element['LASTNAME'], element['NAMESUFFIX']
    ).replace("  ", " ").title().strip()

    # Parse dates.  Must be returned as ISO string, not date object.
    DATES = {'DOB': 'DateOfBirth',
             'LASTVOTEDDATE': 'LastVotedDate'}
    for k in DATES:
        try:
            dt = date(int(element[k][:4]),
                      int(element[k][4:6]),
                      int(element[k][6:8]))
            new[DATES[k]] = str(dt)
            if k == 'DOB':
                new['Age'] = int((datetime.now().date() - dt).days / 365)
        except Exception:
            new[DATES[k]] = None

    # Address fields
    fmt_address, fmt_street = vf_standardize_address(element, usps_key)
    new['Address'] = fmt_address
    new['StreetAddress'] = fmt_street

    # Calculate primary type, etc.
    new['Prime'] = False
    new['FiveYearElections'] = 0
    new['FiveYearPrimaries'] = 0
    for e in element['VoterHistory'].split(';'):
        if e:
            try:
                logging.debug("Looking up election: {}".format(e))
                election = elections[e]
                dd = str(election['Date']).split('-')
                election_date = date(int(dd[0]), int(dd[1]), int(dd[2]))
                years_ago = (datetime.now().date() - election_date).days / 365
                if years_ago < 5:
                    new['FiveYearElections'] += 1
                    if 'Primary' in election['Type']:
                        new['FiveYearPrimaries'] += 1
                        new['Prime'] = True
            except Exception as err:
                # Ignore elections where lookup fails, or date is unknown
                logging.debug("Caught exception: {}".format(str(err)))
                pass
    # results.append(new)
    # return

    # County
    c = element['COUNTYCODE']
    logging.debug("Looking up county: {}".format(c))
    if c in counties:
        new['County'] = counties[c]['Name']
    else:
        new['County'] = None

    logging.debug("Returning: {}".format(new))
    return [new]


def key_by_county_type(row, counties):
    c = row['COUNTYCODE']
    if c in counties:
        county_name = counties[c]['Name']
    else:
        county_name = None

    if row['COUNTYCODE'] in [3, 24, 30, 31, 41, 43]:
        """Nassau and the 5 Boroughs use County, AD, ED key"""
        k = (row['COUNTYCODE'], county_name, row['AD'], None, row['ED'], row['ENROLLMENT'])
    else:
        """Otherwise County, Cousub, Ward, ED"""
        k = (row['COUNTYCODE'], county_name, None, row['WARD'], row['ED'], row['ENROLLMENT'])
    return (k, 1)


def flatten_sum(tup):
    k, v = tup
    output = {}
    for i, field_def in enumerate(COUNTS_SCHEMA.split(',')):
        name, typ = field_def.split(':')
        if i < len(k):
            output[name] = k[i]
        else:
            output[name] = v
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
        '--job_name=voter-pipeline'])

    #if not known_args.usps_key:
    #    raise Exception("Provide USPS API key.")

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:

       # TODO: Select rather than hard-code bucket/file name
        raw = (p
            | "AllNYSVoters_2017-12-27.csv" >> beam.io.ReadFromText(
                    "gs://upload-raw/AllNYSVoters_2017-12-27.csv")
            | "DictFromRawLine" >> beam.ParDo(DictFromRawLine()))

        elections = (p
            | "Voter.ElectionCodes" >> beam.io.Read(
                beam.io.BigQuerySource(
                    table='Voter.ElectionCodes',
                    validate=True))
            | "beam.Map(make_kv_pair, 'Election')" >> beam.Map(make_kv_pair, 'Election'))

        counties = (p
            | "Voter.CountyCodes" >> beam.io.Read(
                beam.io.BigQuerySource(
                    table='Voter.CountyCodes',
                    validate=True))
            | "beam.Map(make_kv_pair, 'Code')" >> beam.Map(make_kv_pair, 'Code'))

        output = (raw
            | "Voter.Raw" >> beam.io.WriteToBigQuery(
                table='Voter.Raw',
                schema=RAW_VF_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

        output = (raw
            | "key_by_county_type" >> beam.Map(key_by_county_type, beam.pvalue.AsDict(counties))
            | "beam.CombinePerKey" >> beam.CombinePerKey(sum)
            | "flatten_sum" >> beam.Map(flatten_sum)
            | "Voter.Counts" >> beam.io.WriteToBigQuery(
                table='Voter.Counts',
                schema=COUNTS_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

        output = (raw
                  # | "BatchElements" >> beam.BatchElements()
                  # | "BatchRunner" >> beam.ParDo(BatchRunner(), known_args.usps_key)
                  | "build_formatted" >> beam.FlatMap(build_formatted,
                        known_args.usps_key,
                        beam.pvalue.AsDict(elections),
                        beam.pvalue.AsDict(counties))
                  | "Voter.Formatted" >> beam.io.WriteToBigQuery(
                      table='Voter.Formatted',
                      schema=FORMATTED_SCHEMA,
                      write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                      create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
