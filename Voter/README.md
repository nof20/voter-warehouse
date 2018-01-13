# Voter

This pipeline creates tables within the Voter dataset, based on a manual upload
of the New York State Voter File to Google Cloud Storage.

To convert the voter file to UTF-8 and upload to Cloud Storage:

```
iconv -f LATIN1 -t UTF-8 AllNYSVotersWph_12272017.txt > AllNYSVoters_2017-12-27.csv
gsutil cp AllNYSVoters_2017-12-27.csv gs://<your bucket name>/
```

To load the metadata tables to BigQuery:

```
bq load Voter.CountyCodes CountyCodes.data.csv CountyCodes.schema.json
bq load Voter.ElectionCodes ElectionCodes.data.csv ElectionCodes.schema.json
```

Note that the Voter county codes (Voter.CountyCodes) are different to the
Census FIPS county codes (Census.CountyCodes).

Then, to run the pipeline on Cloud Dataflow, and assuming you have the Cloud Dataflow SDK already installed:

```
python pipeline.py -runner=DataflowRunner --temp_location=gs://<temp bucket> --staging_location=gs://<staging bucket> --max_num_workers=21 --disk_size_gb=100
--requirements_file=requirements.txt --usps_key=<API key>
```

This takes about 20 minutes to run over the whole NY State Voter File without
USPS address verification, or an unknown amount of time with it switched on (many hours).

NB users of the Google Cloud Platform free trial are limited to a maximum of
8 consecutive VMs and 2 TB of persistent disk.

To test with a sample dataset in local mode:

```
python pipeline.py -runner=DirectRunner --temp_location=gs://<temp bucket> --staging_location=gs://<staging bucket> --requirements_file=requirements.txt --usps_key=<API key>
```
