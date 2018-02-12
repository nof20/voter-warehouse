# Datasets

## Voter

The Voter dataset is based on the New York State Voter File, as provided
by the State Board of Elections.

This contains, for each registered voter in the State of New York:
* Full Name
* Residence address
* Mailing address
* Date of birth
* Gender
* Enrollment (political party)
* District information (County Code, Election District, Legislative District,
  Town/City code, Ward, Congressional District, Senate District, Assembly District)
* Last Voted Date and Previous Year Voted
* Previous county, address and name
* Technical information about voter registration (registration date, source,
  County VR number, identification requirements, whether the ID requirements were
  met, status, the reason code, inactivation date, purge date, and the State
  Board of Elections ID)
* Information about historic election participation, but critically not who
  they voted for
* Phone number

This is broken out into four tables:

* `Voter.Raw`, the full untouched voter file from the Board of Elections (17.7 million rows, 6.00 GB)
* `Voter.Formatted`, a table containing:
    - Only current active voters
    - Friendlier, formatted name and address
    - Current age
    - Whether the voter is 'Prime' (i.e. voted in a primary in the last five years)
    - Total election participation in the last five years
    - Primary election participation in the last five years
* `Voter.CountyCodes`, a lookup table of BoE county codes
* `Voter.ElectionCodes`, a lookup table of BoE historic election codes

## Census

The Census dataset is based on the 2010 Redistricting Census, available free
online from the US Census Bureau.

This is broken out into four tables:

* `Census.Raw`, the full contents of the New York Redistricting Census (444 MB, 635,000 rows)
* `Census.SummaryLevels`, a lookup table of Census Summary Levels.  Each row in
the `Census.Raw` table might correspond to the whole state, a county, or a smaller
unit.
* `Census.RedistEquiv`, the mapping table between the Census districts and the
State Board of Elections Election Districts.
* `Census.VTDFormatted`, a table containing:
    - Rows only for Vote Tabulation Districts, which are roughly equivalent to
      Election Districts
    - County and County Subdivision (`COUSUB`) names, as well as codes
    - Total number of adults aged over 18
    - Hispanic adults aged over 18
    - White (European American) non-Hispanic adults aged over 18
    - Black (African American) non-Hispanic adults aged over 18
    - Total number of housing units
    - Occupied housing units
    - Vacant housing units
    - The Election District(s) which map to this Vote Tabulation District.

## Polls

The Polls dataset is based on public poll data, scraped from websites.

At present this contains one table:

* `Polls.GenericCongressionalVote`, scraped from the Real Clear Politics website.

## Results

* `Results.OpenElections` is the full set of results from the NY Open elections project on Github. (255 MB, 1.29 million rows).  Note this data set is produced by a variety of volunteers, and sometimes has gaps and duplicate data (e.g. 'Total' rows included).
* `Results.Curated` is a curated set of election results, taken directly from the New York Board of Elections Website.  This has been reviewed manually to ensure completeness and accuracy, although doesn't cover as many races as OpenElections.

## Finance

* `Finance.State` is a table of contributions made to certain campaigns and reported to the New York State Board of Elections.  Disclosure time limits are set by Election Law and vary depending on the point in the cycle.  Currently the following campaigns are queried:
    - Governor
    - Lieutenant Governor
    - All State Senate seats
    - State Assembly Districts 9 and 75.

Next: see [example queries](ExampleQueries.md).
