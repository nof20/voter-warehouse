# voter-warehouse

A standardized, open source data warehouse of New York political data, powered
by [Google BigQuery](https://cloud.google.com/bigquery).

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

To get started as a user, see [Getting Started](Docs/GettingStarted.md).

To contribute to this project, see [Contributing](Docs/Contributing.md).

## Legal Stuff

Usage of data and code in this project is governed by the Apache 2.0 license: see
[LICENSE](LICENSE).  This project and any individuals or groups who use it are
not endorsed, supported, or promoted by Google.  Voter information may only be
used for an “elections purpose”, as defined in N.Y. Election Law Section 3-103.5.
(An “elections purpose” has traditionally been interpreted broadly and among other
things includes, campaigning, mailings, voter outreach, fundraising and academic
research.)
