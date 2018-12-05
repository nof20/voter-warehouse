"""Process data from the Census.

curl https://www2.census.gov/census_2010/01-Redistricting_File--PL_94-171/New_York/ny2010.pl.zip > ny2010.pl.zip

See p.6-21, etc.

TODO:
* Handle differently named GCS buckets
* Replace different models for creating BQ Schema with one approach
* Replace dependency on pre-load of Census files to GCS with download, e.g.

    resp = requests.get(...)
    with open(filename, 'wb') as fd:
        for chunk in resp.iter_content(chunk_size=128):
            fd.write(chunk)

* Replace AsList(redist_equiv) with a CoGroupByKey

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

from datetime import date, datetime
from apache_beam.io.gcp.internal.clients import bigquery

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
    ('VTD', 161, 167, 'int'),
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

RE_SCHEMA_FIELDS = [
    ('YR', 'integer', 'nullable'),
    ('STATE', 'integer', 'nullable'),
    ('COUNTY', 'integer', 'nullable'),
    ('COUSUB', 'integer', 'nullable'),
    ('WARD', 'integer', 'nullable'),
    ('AD', 'integer', 'nullable'),
    ('ED', 'integer', 'nullable'),
    ('VTD08', 'integer', 'nullable')
]

FORMATTED_SCHEMA_FIELDS = [
    ('COUNTY', 'integer', 'nullable'),
    ('CountyName', 'string', 'nullable'),
    ('COUSUB', 'integer', 'nullable'),
    ('CousubName', 'string', 'nullable'),
    ('LOGRECNO', 'integer', 'nullable'),
    ('VTD', 'integer', 'nullable'),
    ('TotalOver18', 'integer', 'nullable'),
    ('HispanicOver18', 'integer', 'nullable'),
    ('WhiteNonHispanicOver18', 'integer', 'nullable'),
    ('BlackNonHispanicOver18', 'integer', 'nullable'),
    ('TotalHousingUnits', 'integer', 'nullable'),
    ('OccupiedHousingUnits', 'integer', 'nullable'),
    ('VacantHousingUnits', 'integer', 'nullable'),
    ('ElectionDistricts', 'record', 'repeated', (
        ('WARD', 'integer', 'nullable'),
        ('AD', 'integer', 'nullable'),
        ('ED', 'integer', 'nullable')))
]

REDIST_URL = 'http://www.latfor.state.ny.us/data/2010equiv/REDIST_EQUIV.zip'

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
            if fielddef[3] == 'int':
                try:
                    output[fielddef[0]] = int(output[fielddef[0]])
                except ValueError:
                    output[fielddef[0]] = None
        except ValueError:
            output[fielddef[0]] = None
    logrecno = output.pop('LOGRECNO', None)
    return (logrecno, output)

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

def read_zip_csv(URL):
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
            fh.close()
    del(resp)
    return rows

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

def form_name_lookup(row):
    if (row['SUMLEV'] == 50):
        return [(('COUNTY', row['COUNTY']), row['NAME'])]

    if (row['SUMLEV'] == 60):
        return [(('COUSUB', row['COUSUB']), row['NAME'])]

def form_re_lookup(row):
    ward = int(row['WARD']) if 'WARD' in row else None
    ad = int(row['AD']) if 'AD' in row else None
    return (
        (int(row['COUNTY']), int(row['COUSUB']), int(row['VTD08'])), (ward, ad, int(row['ED']))
    )

def gen_vtd_formatted(row, re_lookup, name_lookup):
    """Generate formatted table of Census data.

    Filter on SUMLEV -> VTD only
    LOGRECNO
    VTD number
    Most relevant P4_HISPANIC18OLDER fields only
    COUNTY number - but Census number or Voter file number?
    COUNTY name
    COUSUB name
    WARD, ED, AD via RedistEquiv
    H1_OCCUPANCY numbers"""

    output = {}
    if row['SUMLEV'] == 710:
        # Copy retained fields
        for field in ['LOGRECNO', 'VTD']:
            output[field] = row[field]

        # Race/ethnicity data
        for tup in [(0, 'TotalOver18'), (1, 'HispanicOver18'),
            (4, 'WhiteNonHispanicOver18'), (5, 'BlackNonHispanicOver18')]:
            output[tup[1]] = row['P4_HISPANIC18OLDER'].split(',')[tup[0]]

        # COUNTY/COUSUB
        output['COUNTY'] = row['COUNTY']
        if ('COUNTY', row['COUNTY']) in name_lookup:
            output['CountyName'] = name_lookup[('COUNTY', row['COUNTY'])]
        else:
            output['CountyName'] = None
        output['COUSUB'] = row['COUSUB']
        if ('COUSUB', row['COUSUB']) in name_lookup:
            output['CousubName'] = name_lookup[('COUSUB', row['COUSUB'])]
        else:
            output['CousubName'] = None

        # Redist Equiv.  This is inefficient - stepping through every entry
        # for each row - but we can't use AsDict because of the 1:many map.
        output['ElectionDistricts'] = []
        for k, v in re_lookup:
            dct = {'WARD': None, 'AD': None, 'ED': None}
            if k == (row['COUNTY'], row['COUSUB'], row['VTD']):
                dct['WARD'] = v[0]
                dct['AD'] = v[1]
                dct['ED'] = v[2]
                output['ElectionDistricts'].append(dct)
        if len(output['ElectionDistricts']) == 0:
            # Repeated fields cannot be null in BQ
            output['ElectionDistricts'].append({'WARD': None, 'AD': None, 'ED': None})

        # Occupancy
        for tup in [(0, "TotalHousingUnits"), (1, "OccupiedHousingUnits"),
            (2, "VacantHousingUnits")]:
            output[tup[1]] = row['H1_OCCUPANCY'].split(',')[tup[0]]

        return [output]



def run(argv=None):
    """Main entry point; defines and runs the pipeline."""
    logging.info("Starting pipeline.")

    parser = argparse.ArgumentParser()
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_args.extend([
        '--project=voterdb-test',
        '--job_name=census-pipeline',
        '--temp_location gs://voterdb-test-dataflow-temp/',
        '--staging_location gs://voterdb-test-dataflow-staging/',
        '--max_num_workers=8',
        '--disk_size_gb=100'])

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

        redist_equiv = (p
            | "read_zip_csv" >> beam.Create(read_zip_csv(REDIST_URL)))

        results = (redist_equiv
            | "Census.RedistEquiv" >> beam.io.WriteToBigQuery(
                table='Census.RedistEquiv',
                schema=gen_schema(RE_SCHEMA_FIELDS),
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

        raw = (
            {'first_file': first_file, 'second_file': second_file, 'third_file': third_file}
            | "beam.CoGroupByKey" >> beam.CoGroupByKey()
            | "flatten_row" >> beam.Map(flatten_row))

        name_lookup = (raw
            | "form_name_lookup" >> beam.FlatMap(form_name_lookup))

        re_lookup = (redist_equiv
            | "form_re_lookup" >> beam.Map(form_re_lookup))

        results = (raw
            | "Census.Raw" >> beam.io.WriteToBigQuery(
                table='Census.Raw',
                schema=gen_bq_schema(),
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))

        results = (raw
            | "gen_vtd_formatted" >> beam.FlatMap(gen_vtd_formatted,
                beam.pvalue.AsList(re_lookup),
                beam.pvalue.AsDict(name_lookup))
            | "Census.VTDFormatted" >> beam.io.WriteToBigQuery(
                table='Census.VTDFormatted',
                schema=gen_schema(FORMATTED_SCHEMA_FIELDS),
                write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
