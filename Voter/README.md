# Voter

## Preparation and Metadata

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

## Pipeline

![Pipeline image](pipeline.png)

The pipeline works as follows:
* `Voter.CountyCodes` and `Voter.ElectionCodes` are loaded from BigQuery and converted to key, value (K, V) dictionaries
* The main Voter File CSV is loaded from Cloud Storage and some basic type conversion done to create a "raw" dataset
* Then three steps occur in parallel:
    - The raw dataset is stored to BigQuery
    - A function called `build_formatted` creates friendlier data, e.g. builds whole addresses and names, and calculates the number of recent elections the voter has participated in
    - Counts of voters per party and election district are calculated.

To run the pipeline on Cloud Dataflow, and assuming you have the Cloud Dataflow SDK already installed:

```
python pipeline.py \
  --runner=DataflowRunner \
  --temp_location=gs://<temp bucket> \
  --staging_location=gs://<staging bucket> \
  --max_num_workers=8 \
  --disk_size_gb=100
```

This takes about 30 minutes to run over the whole NY State Voter File on Cloud Dataflow.

NB users of the Google Cloud Platform free trial are limited to a maximum of 8 consecutive VMs and 2 TB of persistent disk.

To test with a sample dataset in local mode, edit the address of the voter file in the code to point to a cut-down version, and run:

```
python pipeline.py \
  --runner=DirectRunner \
  --temp_location gs://voterdb-test-dataflow-temp/ \
  --staging_location gs://voterdb-test-dataflow-staging/ \
```
