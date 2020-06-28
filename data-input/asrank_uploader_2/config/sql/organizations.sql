-- ORGS TABLE
DROP TABLE IF EXISTS organizations;
CREATE TABLE organizations(
  id serial NOT NULL PRIMARY KEY,
  org_id character varying,
  org_name character varying,
  rank bigint default 0,
  seen boolean default true,
  source character varying,
  range json,
  asns json,
  cone json,
  country json,
  members json,
  asndegree json,
  orgdegree json,
  announcing json,
  valid_date_first timestamp with time zone NOT NULL default NOW(),
  valid_date_last timestamp with time zone NOT NULL default NOW()
);
CREATE INDEX orgs_org_idx ON organizations (org_id);
CREATE INDEX orgs_name_idx ON organizations (org_name);