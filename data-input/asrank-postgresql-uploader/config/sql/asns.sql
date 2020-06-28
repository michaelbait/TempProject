-- ASNS TABLE
DROP TABLE IF EXISTS asns;
CREATE TABLE asns(
  id serial NOT NULL PRIMARY KEY,
  hid character varying default '512',
  asn character varying,
  name character varying,
  source character varying,
  org_id character varying ,
  org_name character varying ,
  country character varying,
  country_name character varying,
  latitude float default 0.0,
  longitude float  default 0.0,
  rank integer default 0,
  customer_cone_asns bigint default 0,
  customer_cone_prefixes bigint default 0,
  customer_cone_addresses bigint default 0,
  degree_peer bigint default 0,
  degree_global bigint default 0,
  degree_customer bigint default 0,
  degree_sibling bigint default 0,
  degree_transit bigint default 0,
  degree_provider bigint default 0,
  address_country character varying(10) default 'global',
  address_type smallint default 4,
  date_from timestamp with time zone NOT NULL default NOW(),
  date_to timestamp with time zone NOT NULL default NOW(),
  ts timestamp with time zone NOT NULL default NOW()
);
CREATE INDEX asns_asn_idx ON asns (asn);
CREATE INDEX asns_name_idx ON asns (name);
CREATE INDEX asns_org_idx ON asns (org_id);
CREATE INDEX asns_org_name_idx ON asns (org_name);