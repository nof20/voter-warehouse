# VOTER

This pipeline creates tables within the VOTER dataset.

To run on Cloud Dataflow, and assuming you have the Cloud Dataflow SDK already installed:

```
python pipeline.py -runner=DataflowRunner --temp_location=gs://<temp bucket> --staging_location=gs://<staging bucket> --max_num_workers=21 --requirements_file=requirements.txt --usps_key=<API key>
```

To test in local mode:

```
python pipeline.py -runner=DirectRunner --temp_location=gs://<temp bucket> --staging_location=gs://<staging bucket> --requirements_file=requirements.txt --usps_key=<API key>
```
