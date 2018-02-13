# Example queries

## Voter

Get data for a specific voter:

```
SELECT FullName, Gender, Enrollment, DateOfBirth, FiveYearElections, FiveYearPrimaries
FROM Voter.Formatted
WHERE FullName = "John Simonian"
AND County = "Queens"
```

|Row|FullName|Gender|Enrollment|DateOfBirth|FiveYearElections|FiveYearPrimaries|
|---|--------|------|----------|-----------|-----------------|-----------------|
|1|John Simonian|M|DEM|1956-02-27|10|5|

Get summary of voters by enrollment (party) in a given district:

```
SELECT Enrollment, COUNT(*) AS Num
FROM Voter.Formatted
WHERE SenateDistrict = 31
GROUP BY Enrollment
```

|Row|Enrollment|Num|
|---|----------|---|
|1|DEM|156765|
|2|IND|3848|
|3|REP|11024|
|4|BLK|28688|
|5|CON|331|
|6|WOR|423|
|7|GRE|495|
|8|OTH|57|
|9|WEP|85|
|10|REF|13|

Get summary of State Senate districts overlapping with AD 75, with voter count in each one:

```
SELECT SD, COUNT(*) AS Num
FROM Voter.Raw
WHERE AD = 75
AND STATUS = "ACTIVE"
GROUP BY 1
```

|Row|SD|Num|
|---|--|---|
|1|31|1680|
|2|28|19293|
|3|27|65896|

## Census

Get data for a specific district:

```
#standardSQL
SELECT CountyName, CousubName, TotalOver18, HispanicOver18,
  WhiteNonHispanicOver18, BlackNonHispanicOver18, TotalHousingUnits,
  OccupiedHousingUnits, VacantHousingUnits, e.AD, e.ED
FROM Census.VTDFormatted v, UNNEST(v.ElectionDistricts) e
WHERE e.AD = 67
```

|Row|CountyName|CousubName|TotalOver18|HispanicOver18|WhiteNonHispanicOver18|BlackNonHispanicOver18|TotalHousingUnits|OccupiedHousingUnits|VacantHousingUnits|AD|ED|
|---|----------|----------|-----------|--------------|----------------------|----------------------|-----------------|--------------------|------------------|--|--|
|1|New York County|Manhattan borough|1100|51|930|14|791|716|75|67|48|
|2|New York County|Manhattan borough|781|46|666|15|554|511|43|67|103|
|3|New York County|Manhattan borough|1354|121|979|70|1154|994|160|67|12|
|4|New York County|Manhattan borough|7|1|0|6|0|0|0|67|118|
|5|New York County|Manhattan borough|1425|130|1020|59|1042|990|52|67|6|

## Results

Get 2016 election vote totals for Suffolk County only, Congressional Districts 1-3:

```
#standardSQL
SELECT office, district, candidate, ARRAY_TO_STRING(ARRAY_AGG(party), ", ") AS Parties, sum(votes) AS Votes
FROM Results.Curated
WHERE county='Suffolk'
AND election_date='2016-11-08'
AND candidate NOT IN ('Blank', 'Scattering', 'Void', 'Total')
AND district IN (1, 2, 3)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
```

|Row|office|district|candidate|Parties|Votes|
|---|------|--------|---------|-------|-----|
|1|U.S. House|1|Anna E. Throne-Holst|DEM, WEP, WOR|135278.0|
|2|U.S. House|1|Lee M. Zeldin|REP, IND, CON, REF|188499.0|
|3|U.S. House|2|Du Wayne Gregory|DEM, IND, WOR, WEP|86835.0|
|4|U.S. House|2|Peter T. King	CON, REP/TRP, REF|114559.0|
|5|U.S. House|3|Jack M. Martins|REP, CON, REF|58857.0|
|6|U.S. House|3|Thomas R. Suozzi|DEM|56166.0|

## Finance

Generate a summary by year of fundraising for State Senate District 22:

```
SELECT FilingYear, FilerID, FilerName, FORMAT("$%'.0f", SUM(Amt)) AS Raised
FROM Finance.State
WHERE FilingYear > 2016
AND Office = "State Senator"
AND Dist = 22
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
```

|Row|FilingYear|FilerID|FilerName|Raised|
|---|----------|-------|---------|------|
|1|2017|A13015|FRIENDS OF MARTY GOLDEN|$225,920|
|2|2017|A22005|ROSS BARKAN FOR NEW YORK|$16,605|
|3|2018|A13015|FRIENDS OF MARTY GOLDEN|$339,297|
|4|2018|A22005|ROSS BARKAN FOR NEW YORK|$50,500|
