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

```
#standardSQL
SELECT office, district, candidate, sum(votes) AS Votes
FROM Results.Curated
WHERE county='Suffolk'
AND election_date='2016-11-08'
AND candidate NOT IN ('Blank', 'Scattering', 'Void')
AND district IN (1, 2, 3)
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
```

|Row|office|district|candidate|Votes|
|---|------|--------|---------|-----|	 
|1|U.S. House|1|Anna E. Throne-Holst|135278|
|2|U.S. House|1|Lee M. Zeldin|188499|
|3|U.S. House|2|Du Wayne Gregory|86835|
|4|U.S. House|2|Peter T. King|114559|
|5|U.S. House|3|Jack M. Martins|58857|
|6|U.S. House|3|Thomas R. Suozzi|56166|
