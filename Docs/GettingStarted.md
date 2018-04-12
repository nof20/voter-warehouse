# Getting Started

Before we start, some basic information about the platform.

[BigQuery](https://cloud.google.com/bigquery/) is Google's serverless, highly scalable, low cost enterprise data warehouse.  It is different from a traditional database because instead of
paying for a server to run all the time, you only pay when you analyze data,
and the first 1 TB of query and 10 GB of data stored are completely free.

`voter-warehouse` maintains datasets in BigQuery for everyone to use.

## Get setup with Google Cloud

To access the `voter-warehouse` data in BigQuery, you first need to sign up for a
Google Cloud Platform account.  This is free, and you get a further [$300 credit](https://cloud.google.com/free/) for your first year.  To do this:

* Sign in with your Google (e.g. Gmail) account.
* Go to the [Google Cloud Platform Console](http://console.cloud.google.com).
* [Create a project](https://console.cloud.google.com/cloud-resource-manager).  This is your personal workspace; the name doesn't matter.
* Enable billing.  Open the console left side menu and select 'Billing'.  Click the
new billing account button, enter a name and your personal details, then click
'Submit and enable billing.'

While the datasets are maintained by the `voter-warehouse` project, any costs
you incur in excess of the free tier are billed directly to you.

## Get permission on the data

Because `voter-warehouse` includes personally-identifying data, the datasets are
not open to the public.  Email `contact@voterdb.org` to be permissioned, and
provide information about the organizations you're affiliated with.

## Run your first query

BigQuery, like most databases, uses a very common language called SQL (Structured
Query Language).  A simple query might be:

```
#standardSQL
SELECT FullName, Gender, DateOfBirth
FROM `voter-warehouse.Voter.Formatted`
LIMIT 10;
```

This means:
* Tell BigQuery to use standard SQL syntax
* Get the fields "FullName", "Gender", and "DateOfBirth"
* From the table called "Voter.Formatted"
* Up to a maximum of 10 rows.

To run this:
* Go to BigQuery on the web at https://bigquery.cloud.google.com.
* Click the red "Compose Query" button on the top left hand corner.
* Copy and paste ALL of the code above into the Query box.  Be sure to include the first line, "#standardSQL" - this tells BigQuery to use industry standard syntax.
* Click "Run Query".

You should see results in the table at the bottom, which look like this:

| Row |	FullName |	Gender | DateOfBirth |
|-----|----------|---------|-------------|
| 1	  | Mary Smith | F | 1940-11-02	 |
| 2	  | Jack Harken |	M	| 1937-03-17	 |
| 3   |	Johanna Larsson |	F |	1969-11-20	 |
| 4 |	Oleg Klatshkov |	M | 	1956-08-12	 |
| 5 |	Fei Fei Wong |	F |	1948-10-18	 |
| 6	 | Janet De Bono |	F |	1955-01-15	 |
| 7 |	Alessandra Greenberg |	F	| 1964-12-24	 |
| 8 |	Brian Goonan |	M	| 1937-05-14	 |
| 9 |	John F Miller |	M	| 1957-08-31	 |
| 10 |	John Roberts	| M |	1936-02-23	 |

You made it!

To learn more about SQL, you can take the free online SQL Tutorial at [w3schools.com](https://www.w3schools.com/sql/default.asp).

You can learn more about how BigQuery works by following their [Quickstart
Tutorial](https://cloud.google.com/bigquery/quickstart-web-ui).

Next step: read more about the [Datasets](Datasets.md).
