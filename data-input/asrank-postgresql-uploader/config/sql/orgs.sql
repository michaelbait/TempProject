-- ORGS TABLE
DROP TABLE IF EXISTS orgs;
CREATE TABLE orgs(
  id serial NOT NULL PRIMARY KEY,
  hid character varying default '512',
  rank bigint default 0,
  org_id character varying,
  org_name character varying,
  country character varying,
  country_name character varying,
  asn_degree_global bigint default 0,
  asn_degree_transit bigint default 0,
  org_degree_global bigint default 0,
  org_degree_transit bigint default 0,
  customer_cone_asns bigint default 0,
  customer_cone_orgs bigint default 0,
  customer_cone_addresses bigint default 0,
  customer_cone_prefixes bigint default 0,
  number_members bigint default 0,
  number_members_ranked bigint default 0,
  members character varying[],
  address_country character varying(10) default 'global',
  address_type smallint default 4,
  date_from timestamp with time zone NOT NULL default NOW(),
  date_to timestamp with time zone NOT NULL default NOW(),
  ts timestamp with time zone NOT NULL default NOW()
);
CREATE INDEX orgs_org_idx ON orgs (org_id);
CREATE INDEX orgs_name_idx ON orgs (org_name);