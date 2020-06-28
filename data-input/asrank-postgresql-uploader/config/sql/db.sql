-- DATASET TABLE
DROP TABLE IF EXISTS dataset;
CREATE TABLE dataset(
  id serial NOT NULL PRIMARY KEY,
  hid character varying default '512',
  dataset_id character varying,
  clique character varying[],
  asn_ixs character varying[],
  sources jsonb,
  asn_reserved_ranges jsonb,
  asn_assigned_ranges jsonb,
  number_asns bigint default 0,
  number_organizes bigint default 0,
  number_prefixes bigint default 0,
  number_addresses bigint default 0,
  address_family character varying,
  country character varying(10) default 'global',
  type smallint default 4,
  date_from timestamp with time zone NOT NULL default NOW(),
  date_to timestamp with time zone NOT NULL default NOW(),
  time_modified timestamp with time zone NOT NULL default NOW()
);
CREATE INDEX dataset_hid_idx ON dataset (hid);

-- ASNS TABLE
DROP TABLE IF EXISTS asns;
CREATE TABLE asns(
  id serial NOT NULL PRIMARY KEY,
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
  date_from timestamp with time zone NOT NULL default NOW(),
  date_to timestamp with time zone NOT NULL default NOW(),
  time_modified timestamp with time zone NOT NULL default NOW()
);
CREATE INDEX asns_asn_idx ON asns (asn);
CREATE INDEX asns_name_idx ON asns (name);
CREATE INDEX asns_org_idx ON asns (org_id);
CREATE INDEX asns_org_name_idx ON asns (org_name);

-- ORGS TABLE
DROP TABLE IF EXISTS orgs;
CREATE TABLE orgs(
  id serial NOT NULL PRIMARY KEY,
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
  date_from timestamp with time zone NOT NULL default NOW(),
  date_to timestamp with time zone NOT NULL default NOW(),
  time_modified timestamp with time zone NOT NULL default NOW()
);
CREATE INDEX orgs_org_idx ON orgs (org_id);
CREATE INDEX orgs_name_idx ON orgs (org_name);

-- LINKS TABLE
DROP TABLE IF EXISTS links;
CREATE TABLE links(
  id serial NOT NULL PRIMARY KEY,
  asn0 character varying,
  asn1 character varying,
  relationship character varying,
  number_paths bigint default 0,
  locations character varying[],
  date_from timestamp with time zone NOT NULL default NOW(),
  date_to timestamp with time zone NOT NULL default NOW(),
  time_modified timestamp with time zone NOT NULL default NOW()
);
CREATE INDEX link_asn0_idx ON links (asn0);
CREATE INDEX link_asn1_idx ON links (asn1);

-- LINKS TABLE
DROP TABLE IF EXISTS locations;
CREATE TABLE locations(
  id serial NOT NULL PRIMARY KEY,
  lid character varying,
  city character varying,
  country character varying,
  continent character varying,
  region character varying,
  population bigint default 0,
  longitude float default 0.0,
  latitude float default 0.0,
  date_from timestamp with time zone NOT NULL default NOW(),
  date_to timestamp with time zone NOT NULL default NOW(),
  time_modified timestamp with time zone NOT NULL default NOW()
);
CREATE INDEX loc_lid_idx ON locations (lid);
CREATE INDEX loc_city_idx ON locations (city);
