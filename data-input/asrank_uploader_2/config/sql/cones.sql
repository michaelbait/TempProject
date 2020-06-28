-- CONES TABLE
DROP TABLE IF EXISTS cones;
CREATE TABLE cones(
  id serial NOT NULL PRIMARY KEY,
  aid bigint,
  rank bigint,
  asn json,
  cone json,
  asns json,
  pfx json,
  range json,
  valid_date_first timestamp with time zone NOT NULL default NOW(),
  valid_date_last timestamp with time zone NOT NULL default NOW()
);
CREATE INDEX cones_asn_idx ON cones (aid);
