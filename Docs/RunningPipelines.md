# Running the pipelines

Initial implementation of the pipelines had one file per module.  I'm now in
the process of abstracting shared code.

## Census

Data from the 2010 US Redistricting Census.  This module uses the new format.

Download input data from the Census website:
```
curl https://www2.census.gov/census_2010/01-Redistricting_File--PL_94-171/New_York/ny2010.pl.zip > ny2010.pl.zip
unzip ny2010.pl.zip
gsutil cp *.pl gs://upload-raw/census/
```

Load the SummaryLevels metadata:

```
bq load Census.SummaryLevels Data/SummaryLevels.data.csv Data/SummaryLevels.schema.json
```

Run the pipeline:

```
python pipeline.py --module=census --runner=DataflowRunner
```

At present the pipeline relies on the files being present in GCS bucket
`gs://upload-raw/census/`.  The pipeline takes about 14 minutes to complete
on one worker.
