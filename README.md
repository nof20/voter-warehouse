# voter-warehouse

A standardized data warehouse of New York political data, using [Google BigQuery](https://cloud.google.com/bigquery) and [Google Cloud Dataflow](https://cloud.google.com/dataflow).

Principles:
* Free or minimal cost to use
* Easy to query, join and analyze
* Simple, repeatable pipelines to populate data

Datasets:

| Name     | Description               | Source                               | Status          |
|----------|---------------------------|--------------------------------------|-----------------|
| Voter    | Voter rolls               | NY State Board of Elections, by mail | OK              |
| Census   | 2010 Redistricting Census | US Census Bureau website             | OK              |
| Polls    | Various polls             | Scraped from websites                | In Progress     |
| Results  | Historic election results | [OpenElections](http://github.com/openelections), NY State Board of Elections website | Not Started |
| Finance  | Donations and campaign spending | NY State Board of Elections website | Not Started |
