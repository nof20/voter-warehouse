# FinanceMatch

Match (State) Finance data to the Voter File, in order to identify potential donors.

## Pipeline

To run the pipeline:

```
python pipeline.py \
  --runner=DataflowRunner \
  --temp_location=gs://<temp bucket> \
  --staging_location=gs://<staging bucket> \
  --requirements_file=requirements.txt
```

At present this pipeline has district number and type hard-coded; this should be extended in future.
