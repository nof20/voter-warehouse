# Census

Data from the 2010 US Redistricting Census.

To run the pipeline and import Census data from Google Cloud Storage:

```
python pipeline.py --runner=DataflowRunner --temp_location gs://voterdb-test-dataflow-temp/ --staging_location gs://voterdb-test-dataflow-staging/
```

To load the SummaryLevels and CountyCodes metadata:

```
bq load Census.SummaryLevels SummaryLevels.data.csv SummaryLevels.schema.json
bq load Census.CountyCodes CountyCodes.data.csv CountyCodes.schema.json
```
