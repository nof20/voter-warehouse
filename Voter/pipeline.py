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

from pyusps import address_information
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
    "WARD:STRING,CD:INTEGER,SD:INTEGER,AD:INTEGER,LASTVOTEDDATE:STRING,"
    "PREVYEARVOTED:STRING,PREVCOUNTY:STRING,PREVADDRESS:STRING,"
    "PREVNAME:STRING,COUNTYVRNUMBER:STRING,REGDATE:STRING,VRSOURCE:STRING,"
    "IDREQUIRED:STRING,IDMET:STRING,STATUS:STRING,REASONCODE:STRING,"
    "INACT_DATE:STRING,PURGE_DATE:STRING,SBOEID:STRING,"
    "VoterHistory:STRING,Phone:STRING")

FORMATTED_SCHEMA = (
    "FullName:STRING,Gender:STRING,Enrollment:STRING,OtherPartyEnrollment:STRING,"
    "ElectionDistrict:INTEGER,LegislativeDistrict:INTEGER,TownCityCode:STRING,"
    "Ward:STRING,CongressionalDistrict:INTEGER,SenateDistrict:INTEGER,"
    "AssemblyDistrict:INTEGER,PreviousName:STRING,CountyVoterNumber:STRING,"
    "VoterRegistrationSource:STRING,IDRequired:STRING,IDMet:STRING,"
    "Status:STRING,StatusReasonCode:STRING,SBOEID:STRING,DateOfBirth:DATE,"
    "LastVotedDate:DATE,InactivatedDate:DATE,PurgedDate:DATE,Age:INTEGER,"
    "Address:STRING,StreetAddress:STRING,VoterHistory:STRING")

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
                            dct[name] = int(data[i].encode('utf-8').strip())
                    except Exception:
                        dct[name] = None
                yield dct
        except Exception:
            logging.error("Error in csv.reader")


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
        # addr = {'address': address, 'city': row['RCITY'], 'state': 'NY'}
        # result = address_information.verify(usps_key, addr)
        # zip4 = "-{}".format(result['zip4']) if result['zip4'] else ''
        # fmt_address = "{}, {} {} {}{}".format(
        #    result['address'],
        #    result['city'],
        #    result['state'],
        #    result['zip5'],
        #    zip4)
        fmt_address = ", ".join(address, row['RCITY'], "NY {}".format(row['RZIP5']))
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
        # addr = {'address': street, 'city': row['RCITY'], 'state': 'NY'}
        # result = address_information.verify(usps_key, addr)
        # zip4 = "-{}".format(result['zip4']) if result['zip4'] else ''
        # fmt_street = "{}, {} {} {}{}".format(
        #    result['address'],
        #    result['city'],
        #    result['state'],
        #    result['zip5'],
        #    zip4)
        fmt_street = ", ".join(street, row['RCITY'], "NY {}".format(row['RZIP5']))
    except Exception:
        fmt_street = None

    return fmt_address, fmt_street


#def build_formatted(element, usps_key, results):
def build_formatted(element, usps_key):

    """Generate row in formatted table."""

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
        'STATUS': 'Status',
        'REASONCODE': 'StatusReasonCode',
        'SBOEID': 'SBOEID',
        'VoterHistory': 'VoterHistory'}
    new = {RETAIN[k]: element[k] for k in RETAIN}

    # Name.
    new['FullName'] = "{} {} {} {}".format(
        element['FIRSTNAME'], element['MIDDLENAME'],
        element['LASTNAME'], element['NAMESUFFIX']
        ).replace("  ", " ").title()

    # Parse dates.  Must be returned as ISO string, not date object.
    DATES = {'DOB': 'DateOfBirth',
        'LASTVOTEDDATE': 'LastVotedDate',
        'INACT_DATE': 'InactivatedDate',
        'PURGE_DATE': 'PurgedDate'}
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

    #results.append(new)
    #return
    return new

class BatchRunner(beam.DoFn):

    def process(self, batch, usps_key):
        threads = deque()
        results = []
        for row in batch:
            if len(threads) > 50:
                t = threads.popleft()
                t.join()

            t = threading.Thread(target=build_formatted,
                    args=(row, usps_key, results))
            t.start()
            threads.append(t)

        while threads:
            t = threads.popleft()
            t.join()

        for r in results:
            yield r

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

    if not known_args.usps_key:
        raise Exception("Provide USPS API key.")

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:

       # TODO: Select rather than hard-code bucket/file name
        raw = (p
               | "AllNYSVoters_2017-12-27.csv" >> beam.io.ReadFromText("gs://upload-raw/AllNYSVoters_2017-12-27.csv")
               | "DictFromRawLine" >> beam.ParDo(DictFromRawLine()))

        output = (
            raw | "Voter.Raw" >> beam.io.WriteToBigQuery(
                table='Voter.Raw',
                schema=RAW_VF_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

        output = ( raw
            # | "BatchElements" >> beam.BatchElements()
            # | "BatchRunner" >> beam.ParDo(BatchRunner(), known_args.usps_key)
            | "build_formatted" >> beam.Map(build_formatted, known_args.usps_key)
            | "Voter.Formatted" >> beam.io.WriteToBigQuery(
                table='Voter.Formatted',
                schema=FORMATTED_SCHEMA,
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
