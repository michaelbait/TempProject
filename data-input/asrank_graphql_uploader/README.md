## ASRankUploader-3.1

####ASRank data uploader for Multidatasets Graphql API.

Dataset №1
2016-01-11 <-> 2016.02.01

Dataset №2
2016.05.01 <-> 2016.06.01

2016-01-11 <-> 2016.06.01

###### Test postgersql orgs date range query
```
SELECT * FROM organizations WHERE valid_date_first >= '2017-01-01' AND valid_date_last <= '2017-05-01'
```

DATASET TABLE
==============================================
Select a dataset by its id
--------------------------
SELECT * FROM dataset WHERE dataset_id = '20190101';

Select and check if sources field is not empty
--------------------------
select * from dataset where sources::text <> '[]'::text;

ASNS TABLE
==============================================
Check table existing in database
--------------------------
SELECT EXISTS (SELECT 1 FROM pg_tables WHERE schemaname = 'public' AND tablename = 'asns');

Select with sorting by field "rank" and desc direction.
--------------------------
SELECT * FROM asns ORDER BY rank DESC;

Select with pagination from 40 to 50 and 10 rows
--------------------------
SELECT * FROM asns ORDER BY rank DESC OFFSET 40 LIMIT 10;

Grouping by org_id with aggregate count by org_name
--------------------------
SELECT org_id, count(org_name) FROM asns GROUP BY org_id;

Filtering asns by rank filed
--------------------------
SELECT * FROM asns WHERE rank > 0 AND rank < 20 limit 10;

Select with case-insensitive
--------------------------
SELECT * FROM asns WHERE LOWER(name)=LOWER('ACI-NET')

Select asns and links if asn.asn and links.asn0 = 3356
--------------------------
SELECT 
  asns.asn, 
  asns.name, 
  asns.org_id, 
  asns.org_name,
  links.asn0, 
  links.locations, 
  links.relationship, 
  links.number_paths
FROM 
  public.asns, 
  public.links
WHERE 
  asns.asn = '3356' AND asns.asn = links.asn0;

Select from asns with join from links if asns.asn = links.asn0
---------------------------
SELECT asn, org_id, org_name, links.relationship FROM asns LEFT JOIN links ON asns.asn = links.asn0 LIMIT 10;

Select with date filtering 
---------------------------
SELECT asn, name, org_id, org_name, ts FROM asns WHERE ts > '2019-01-31 20:00:00' LIMIT 100;
SELECT asn, name, org_id, org_name, ts FROM asns WHERE ts BETWEEN '2019-01-31 20:00:00' AND '2019-01-31 21:00:00' LIMIT 10;
SELECT count(1) FROM asns WHERE ts > now() - interval '1 week';

ORGS TABLE
==============================================
Select from orgs and limit result to 10 rows
--------------------------- 
SELECT * FROM orgs LIMIT 10;

Select from orgs and filter by country (only US)
--------------------------- 
SELECT * FROM orgs WHERE lower(country) = lower('US') LIMIT 10;

Select from orgs and sort by rank field
---------------------------
SELECT * FROM orgs order by rank asc LIMIT 10;

Select from orgs and filter by org_id equal to LPL-141-ARIN
---------------------------
SELECT * FROM orgs WHERE upper(org_id) = upper('LPL-141-ARIN') LIMIT 10;

Select from orgs and filter by org_id contains to "ARIN"
---------------------------
SELECT * FROM orgs WHERE org_id ILIKE '%ARIN%' LIMIT 10;

Select from orgs and filter by org_id start with "ARIN"
---------------------------
SELECT * FROM orgs WHERE org_id ILIKE 'ARIN%' LIMIT 10;

Select members by org id 
---------------------------
SELECT members FROM orgs WHERE lower(org_id) = lower('LPL-141-ARIN') LIMIT 10;

Select from orgs where members field equal 2
---------------------------
SELECT * FROM orgs WHERE '2' = ANY(members);

Search for an organization by name "level"
---------------------------
SELECT * FROM orgs WHERE org_name ilike '%level%' LIMIT 10;

Select members for orgs searched by org name 
--------------------------
SELECT members FROM orgs WHERE org_name ilike '%level%' LIMIT 10;

LOCATIONS TABLE
==============================================
Select locations
-------------------------
SELECT * FROM locations limit 10;

Select locations filtered by lid field
-------------------------
SELECT * FROM locations WHERE lower(lid) = lower('Austin-MN-US') limit 10;

LINKS TABLE
==============================================
Select all links
------------------------
SELECT * FROM links limit 10;

List the links between two ASNs
------------------------
SELECT * FROM links WHERE asn0 = '1239' AND  asn1 = '1299';

Select links by asn
------------------------
SELECT * FROM links WHERE asn0 = '1299' OR  asn1 = '1299';

Select neighbors for org name 'LPL-141-ARIN'
------------------------
SELECT 
  asns.org_id, 
  asns.org_name, 
  links.asn0, 
  links.asn1, 
  links.relationship, 
  links.number_paths, 
  links.locations, 
  links.date, 
  links.ts
FROM 
  public.asns, 
  public.links
WHERE 
  links.asn0 IN (select asn from asns where lower(asns.org_id) = lower('LPL-141-ARIN')) 
  AND asns.org_id <> ''
  AND asns.asn = links.asn1
  ORDER BY org_id LIMIT 1000;


Count grouping by count neighbors for org_id 'LPL-141-ARIN'
---------------------------
SELECT 
  asns.org_id, 
  COUNT(links.asn1)
FROM 
  public.asns, 
  public.links
WHERE 
  links.asn0 IN (select asn from asns where lower(asns.org_id) = lower('LPL-141-ARIN')) 
  AND asns.org_id <> ''
  AND asns.asn = links.asn1
  GROUP BY org_id  LIMIT 1000;
  
 
 Test ranked links
 ------------------------- 
SELECT asn0, asn1, rank0, rank1, relationship,
CASE
    WHEN rank0 <= rank1 THEN rank0
    WHEN rank1 <= rank0 THEN rank1
    ELSE 99999999
END AS trank
FROM links
WHER asn0 = 3356 OR asn1 = 3356