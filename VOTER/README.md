# VOTER

This pipeline creates tables within the VOTER dataset.

To run on Python 2, and assuming you have the Cloud Dataflow SDK already installed:

```
python pipeline.py -runner DataflowRunner --temp_location gs://<temp bucket> --staging_location gs://<staging bucket>
```
