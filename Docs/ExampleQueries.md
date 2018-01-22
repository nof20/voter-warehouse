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
