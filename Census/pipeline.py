"""Process data from the Census.

curl https://www2.census.gov/census_2010/01-Redistricting_File--PL_94-171/New_York/ny2010.pl.zip > ny2010.pl.zip

See p.6-21, etc.

"""

from __future__ import absolute_import

import argparse
import logging
import csv

from datetime import date, datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions

FIRST_FILE_FIELDS = [
    ('SUMLEV', 8, 11, 'int'),
    ('LOGRECNO', 18, 25, 'int'),
    ('STATE', 27, 29, 'int'),
    ('COUNTY', 29, 32, 'int'),
    ('COUSUB', 36, 41, 'int'),
    ('CBSA', 112, 117, 'str'),
    ('METDIV', 119, 124, 'str'),
    ('CSA', 124, 127, 'str'),
    ('VTD', 161, 167, 'str'),
    ('VTDI', 167, 168, 'str'),
    ('NAME', 226, 316, 'str'),
    ('INTPTLAT', 336, 347, 'str'),
    ('INTPTLON', 347, 359, 'str'),
    ('LSADC', 359, 361, 'str')
]

SECOND_THIRD_FILE_FIELDS = {
    2: [
        ('LOGRECNO', 4, 5, 'int'),
        ('P1_RACE', 5, 76, 'array'),
        ('P2_HISPANIC', 76, 149, 'array')
        ],
    3: [
        ('LOGRECNO', 4, 5, 'int'),
        ('P3_RACE18OLDER', 5, 76, 'array'),
        ('P4_HISPANIC18OLDER', 76, 149, 'array'),
        ('H1_OCCUPANCY', 149, 152, 'array')
        ]
}

def gen_bq_schema():
    fields = {}
    fields.update({tup[0]: tup[3] for tup in FIRST_FILE_FIELDS})
    fields.update({tup[0]: tup[3] for tup in SECOND_THIRD_FILE_FIELDS[2]})
    fields.update({tup[0]: tup[3] for tup in SECOND_THIRD_FILE_FIELDS[3]})
    field_defs = []
    for k, v in fields.items():
        if v == 'int':
            field_defs.append("{}:{}".format(k, 'INTEGER'))
        else:
            field_defs.append("{}:{}".format(k, 'STRING'))
    return ",".join(field_defs)


# Process first file
def process_first_file(line):
    output = {}
    for fielddef in FIRST_FILE_FIELDS:
        output[fielddef[0]] = line[fielddef[1]:fielddef[2]].strip()
        if fielddef[3] == 'int':
            try:
                output[fielddef[0]] = int(output[fielddef[0]])
            except ValueError:
                output[fielddef[0]] = None
    logrecno = output.pop('LOGRECNO', None)
    return (logrecno, output)

def process_second_third_file(line, filenum):
    output = {}
    cols = line.split(',')
    for fielddef in SECOND_THIRD_FILE_FIELDS[filenum]:
        try:
            output[fielddef[0]] = ','.join(i for i in cols[fielddef[1]:fielddef[2]])
        except ValueError:
            output[fielddef[0]] = None
    logrecno = output.pop('LOGRECNO', None)
    return (logrecno, output)

# def filter_cousub_name(tup):
#     logrecno, dct = tup
#     if dct['SUMLEV'] == 60:
#        return (logrecno, dct['NAME'])

def flatten_row(row):
    # Row after CoGroupByKey looks like (logrecno, {'first_file': {...}, 'second_file': ...})
    #logging.info("Got row: {}".format(row))
    logrecno, dct = row
    output = {'LOGRECNO': logrecno}
    for index in ['first_file', 'second_file', 'third_file']:
        for entry in dct[index]:
            for k, v in entry.items():
                output[k] = v
    return output

def run(argv=None):
    """Main entry point; defines and runs the pipeline."""
    logging.info("Starting pipeline.")

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--project=voterdb-test',
        '--job_name=census-pipeline'])

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:

       # TODO: Select rather than hard-code bucket/file name
        first_file = (p
            | "nygeo2010.pl" >> beam.io.ReadFromText("gs://upload-raw/census/nygeo2010.pl")
            | "process_first_file" >> beam.Map(process_first_file))

        second_file = (p
            | "ny000012010.pl" >> beam.io.ReadFromText("gs://upload-raw/census/ny000012010.pl")
            | "process_second_file" >> beam.Map(process_second_third_file, 2))

        third_file = (p
            | "ny000022010.pl" >> beam.io.ReadFromText("gs://upload-raw/census/ny000022010.pl")
            | "process_third_file" >> beam.Map(process_second_third_file, 3))

        results = (
            {'first_file': first_file, 'second_file': second_file, 'third_file': third_file}
            | "beam.CoGroupByKey" >> beam.CoGroupByKey()
            | "flatten_row" >> beam.Map(flatten_row)
            | "beam.io.WriteToBigQuery" >> beam.io.WriteToBigQuery(
                table='Census.Raw',
                schema=gen_bq_schema(),
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
