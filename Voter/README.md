# VOTER

This pipeline creates tables within the VOTER dataset, based on a manual upload
of the New York State Voter File to Google Cloud Storage.

To run on Cloud Dataflow, and assuming you have the Cloud Dataflow SDK already installed:

```
python pipeline.py -runner=DataflowRunner --temp_location=gs://<temp bucket> --staging_location=gs://<staging bucket> --max_num_workers=21 --disk_size_gb=100
--requirements_file=requirements.txt --usps_key=<API key>
```

This takes about 5.5 hrs to run, because of the slowness of the USPS API.
NB users of the Google Cloud Platform free trial are limited to a maximum of
8 consecutive VMs and 2 TB of persistent disk.

To test in local mode:

```
python pipeline.py -runner=DirectRunner --temp_location=gs://<temp bucket> --staging_location=gs://<staging bucket> --requirements_file=requirements.txt --usps_key=<API key>
```

To load the metadata tables:

```
bq load Voter.CountyCodes CountyCodes.data.csv CountyCodes.schema.json
```
