"""Scrape and analyze campaign finance data.

"""

from __future__ import absolute_import

import argparse
import logging
import re
import sys
import apache_beam as beam
from pyusps import address_information


from datetime import date, datetime
from requests import post
from bs4 import BeautifulSoup
from apache_beam.io.gcp.internal.clients import bigquery
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions


SCHEMA_FIELDS = [
    ('Active', 'string', 'nullable'),
    ('Dist', 'integer', 'nullable'),
    ('FilerID', 'string', 'nullable'),
    ('FilerName', 'string', 'nullable'),
    ('FilerCommittee', 'string', 'nullable'),
    ('Office', 'string', 'nullable'),
    ('Amt', 'float', 'nullable'),
    ('ContributorName', 'string', 'nullable'),
    ('ContributorAddr1', 'string', 'nullable'),
    ('ContributorAddr2', 'string', 'nullable'),
    ('Date', 'date', 'nullable'),
    ('FilingYear', 'integer', 'nullable'),
    ('ReportCode', 'string', 'nullable'),
    ('Sched', 'string', 'nullable')
]

QUERIES = [
    ('Governor', ''),
    ('Lt. Governor', ''),
    ('State Senator', '*'),
    ('Member of Assembly', '9'),
    ('Member of Assembly', '75'),
    ('District Leader', '75'),
    ('District Leader', '67'),
    ('City Council', '3'),
    ('City Council', '6')
]

NYS_BOE_OFFICE_CODES = {
    "Governor": "04",
    "Lt. Governor": "05",
    "Comptroller": "06",
    "Attorney General": "07",
    "U.S. Senator": "08",
    "Sup. Court Justice": "09",
    "State Senator": "11",
    "Member of Assembly": "12",
    "State Committee": "13",
    "Judicial Delegate": "16",
    "Alt Judicial Del.": "17",
    "Chairperson": "18",
    "City Manager": "19",
    "Council President": "20",
    "County Executive": "21",
    "Mayor": "22",
    "President": "23",
    "Supervisor": "24",
    "Sheriff": "25",
    "District Attorney": "26",
    "County Legislator": "27",
    "County Court Judge": "28",
    "Surrogate Court Judge": "29",
    "Family Court Judge": "30",
    "Party Committee Member": "31",
    "City Council": "32",
    "Village Trustee": "33",
    "Village Justice": "34",
    "Clerk": "35",
    "Town Justice": "36",
    "Town Council": "37",
    "Receiver of Taxes": "38",
    "Highway Superintendent": "39",
    "Alderperson": "40",
    "Treasurer": "41",
    "Assessor": "42",
    "Borough President": "43",
    "District Leader": "44",
    "Comptroller": "45",
    "Coroner": "46",
    "County Representative": "47",
    "Public Advocate": "49",
    "Councilman": "50",
    "Trustee": "51",
    "Town Board": "52",
    "Legislator": "53",
    "Legislative District": "54",
    "City Chamberlain": "55",
    "City Council President": "56",
    "City Court Judge": "57",
    "Pres. Common Council": "58",
    "Clerk/Collector": "59",
    "Civil Court Judge": "60",
    "Trustee of School Funds": "61",
    "County Committee": "62",
    "Commissioner of Education": "63",
    "Commissioner of Public Works": "64",
    "Common Council": "65",
    "District Court Judge": "66",
    "Commissioner of Finance": "67",
    "Citizen's Review Board Member": "68",
    "Town Clerk/Tax Collector": "69",
    "Town Tax Collector": "70",
    "Controller": "71",
    "City School Board": "72",
    "Collector": "73",
    "Commissioner of Schools": "74",
    "County Clerk": "75",
    "Town Clerk": "76",
    "Village Clerk": "77",
    "County Treasurer": "78",
    "Town Treasurer": "79",
    "Village Treasurer": "80",
    "City Treasurer": "81",
    "Town Supervisor": "82"
}

YEARS = [2012, 2013, 2014, 2015, 2016, 2017, 2018]


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
                    output[name] = row[name].encode(
                        'utf-8', errors='ignore').strip()
                except Exception:
                    output[name] = None
            else:
                output[name] = None
        elif typ == 'integer':
            try:
                output[name] = int(row[name].strip())
            except Exception:
                output[name] = None
        elif typ == 'float':
            try:
                output[name] = float(row[name].strip())
            except Exception:
                output[name] = None

    return output


def get_filer_status(filerid):
    base_url = "http://www.elections.ny.gov:8080/plsql_browser/getfiler2_loaddates"
    resp = post(base_url, data={'filerid_in': filerid})
    resp.raise_for_status()
    results = {}
    results['FilerID'] = filerid
    m = re.search('Status = (\w+)', resp.text)
    if m:
        results['Status'] = m.group(1)
    return results


def get_all_filers(office, district):
    # See https://www.elections.ny.gov/officetext.html
    logging.info(
        "About to get_all_filers for office {}, district {}".format(
            office, district))
    url = "http://www.elections.ny.gov:8080/plsql_browser/registrantsbycounty_new"
    office_in = NYS_BOE_OFFICE_CODES[office]
    if office == 'City Council':
        # Manhattan only
        county = '31'
    else:
        county = 'ALL'
    data = {'OFFICE_IN': office_in,
            'DISTRICT_IN': district,
            'county_IN': county,
            'municipality_in': ''}
    resp = post(url, data=data)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text)
    tabs = soup.find_all('table')
    tab_sizes = {}
    for i, tab in enumerate(tabs):
        rows = tab.find_all('tr')
        row_num = 0
        for row in rows:
            row_num += 1
        tab_sizes[row_num] = i
    largest_index = tab_sizes[sorted(tab_sizes.keys(), reverse=True)[0]]

    # Get table headings
    headings = []
    for header in tabs[largest_index].find_all('th'):
        headings.append(header.text)
    if not headings:
        raise Exception("No table headings found.")

    # Get table data
    results = []
    for row in tabs[largest_index].find_all('tr'):
        cells = row.find_all('td')
        if len(cells) == len(headings):
            row_data = {}
            for i, cell in enumerate(cells):
                m = re.search('type="SUBMIT" value="(.*)"/>',
                              str(cells[i].contents[0]))
                if m:
                    row_data['FilerID'] = m.group(1)
                    row_data['Active'] = get_filer_status(m.group(1))['Status']
                else:
                    row_data[headings[i]] = cell.text
            results.append(row_data)
    return results


class MultiGetFilers(beam.DoFn):
    """Get all filers, handling district wildcards."""

    def process(self, element):
        office = element[0]
        district = element[1]
        if district == '*' and office == 'State Senator':
            districts = [str(1 + i) for i in range(63)]
        else:
            districts = [district]
        for d in districts:
            for f in get_all_filers(office, d):
                yield f


def add_committees(row):
    url = "http://www.elections.ny.gov:8080/plsql_browser/getfiler2_loaddates"
    logging.info("Getting committees for FilerID {}".format(row['FilerID']))
    data = {'filerid_in': row['FilerID']}
    resp = post(url, data=data)
    resp.raise_for_status()
    output = [row]  # always emit original row
    cttee_id = re.search('getfiler2_loaddates\?filerid_in=(.*)"', resp.text)
    cttee_name = re.search('Authorized Committee = (.*)\n', resp.text)
    if cttee_id and cttee_name:
        new = row.copy()
        new['FilerID'] = cttee_id.group(1).strip()
        new['FilerCommittee'] = cttee_name.group(1).strip()
        output.append(new)

    return output


def get_reports(row):
    url = "http://www.elections.ny.gov:8080/plsql_browser/filer_contribution_details"
    logging.info(
        "Getting reports for FilerID {}, Name {}".format(
            row['FilerID'],
            row['Filer Name']))
    results = []
    for year in YEARS:
        data = {
            'filerid_in': row['FilerID'],
            'fyear_in': year,
            'contributor_in': '',
            'amount_in': '0'}
        resp = post(url, data=data)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text)
        tabs = soup.find_all('table')

        # Find largest table
        tab_sizes = {}
        for i, tab in enumerate(tabs):
            rows = tab.find_all('tr')
            row_num = 0
            for tab_row in rows:
                row_num += 1
            tab_sizes[row_num] = i
        largest_index = tab_sizes[sorted(tab_sizes.keys(), reverse=True)[0]]

        # Get table headings
        headings = []
        for header in tabs[largest_index].find_all('th'):
            headings.append(header.text)
        if not headings:
            raise Exception("No table headings found.")

        # Get table data
        for tab_row in tabs[largest_index].find_all('tr'):
            cells = tab_row.find_all('td')
            if (len(cells) == len(headings)) and (
                    'Total Contributions Received' not in cells[1]):
                row_data = {
                    headings[i]: cell.get_text(
                        separator='\n') for i,
                    cell in enumerate(cells)}
                row_data['Date'] = datetime.strptime(
                    row_data['Date'], "%d-%b-%y").strftime("%Y-%m-%d")
                row_data['Amt'] = row_data['Amt'].replace(',', '')
                row_data.update(row)
                results.append(
                    {k.replace(' ', ''): v for k, v in row_data.items()})

    return results


def split_address(row):
    vars = row['Contributor'].strip().split('\n')
    if len(vars) == 3:
        row['ContributorName'] = vars[0]
        row['ContributorAddr1'] = vars[1]
        row['ContributorAddr2'] = vars[2]
    else:
        row['ContributorName'] = row['Contributor']
        row['ContributorAddr1'], row['ContributorAddr2'] = None, None


def address_lookup(batch, usps_key):
    # Form donor addresses
    post_data = []
    if batch is None:
        return []

    for row in batch:
        try:
            addr1 = row['ContributorAddr1']
            if ',' in row['ContributorAddr2']:
                city = row['ContributorAddr2'].split(',')[0]
            else:
                city = row['ContributorAddr2'].split(' ')[0]
            post_data.append({
                'address': addr1,
                'city': city,
                'state': 'NY'
            })
        except Exception as e:
            logging.error("Could not append to post_data in address_lookup: {}".format(e))

    # Submit batch to API
    try:
        recv_data = address_information.verify(usps_key, *post_data)
    except Exception:
        logging.error("Caught exception posting to address_information.verify: {}".format(e))
    # Match
    output = []
    for i, row in enumerate(batch):
        out_dct = row.copy()
        # Try and use formatted address
        try:
            out_dct['ContributorAddr1'] = recv_data[i]['address']
            if isinstance(recv_data[i]['zip5'], int):
                # So defensive
                recv_data[i]['zip5'] = "{:0.0f}".format(recv_data[i]['zip5'])
            out_dct['ContributorAddr2'] = "{}, {} {}".format(
                recv_data[i]['city'], recv_data[i]['state'], recv_data[i]['zip5'])
        except Exception as e:
            # Output from pyusps is Exception not dict, etc.
            logging.error("Caught exception building out_dct in address_lookup: {}".format(e))
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
        '--job_name=finance-pipeline',
        '--temp_location gs://voterdb-test-dataflow-temp/',
        '--staging_location gs://voterdb-test-dataflow-staging/',
        '--max_num_workers=32',
        '--disk_size_gb=50'])

    if not known_args.usps_key:
        raise Exception("Provide usps_key.")

    pipeline_options = PipelineOptions(pipeline_args)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    with beam.Pipeline(options=pipeline_options) as p:
        raw = (p
               | "beam.Create" >> beam.Create(QUERIES)
               | "MultiGetFilers" >> beam.ParDo(MultiGetFilers())
               | "add_committees" >> beam.FlatMap(add_committees)
               | "get_reports" >> beam.FlatMap(get_reports)
               | "split_address" >> beam.Map(split_address)
               | "BatchElements" >> beam.BatchElements(max_batch_size=5)
               | "address_lookup" >> beam.FlatMap(address_lookup, known_args.usps_key)
               | "flatten_format" >> beam.Map(flatten_format)
               | "Finance.State" >> beam.io.WriteToBigQuery(
                   table='Finance.State',
                   schema=gen_schema(SCHEMA_FIELDS),
                   write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
                   create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED))


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()
