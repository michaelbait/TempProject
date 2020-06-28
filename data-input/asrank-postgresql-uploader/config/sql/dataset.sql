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
  address_country character varying(10) default 'global',
  address_type smallint default 4,
  date_from timestamp with time zone NOT NULL default NOW(),
  date_to timestamp with time zone NOT NULL default NOW(),
  ts timestamp with time zone NOT NULL default NOW()
);
CREATE INDEX dataset_hid_idx ON dataset (hid);