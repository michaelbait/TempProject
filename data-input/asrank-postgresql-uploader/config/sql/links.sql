-- LINKS TABLE
DROP TABLE IF EXISTS links;
CREATE TABLE links(
  id serial NOT NULL PRIMARY KEY,
  hid character varying default '512',
  asn0 character varying,
  asn1 character varying,
  relationship character varying,
  number_paths bigint default 0,
  locations character varying[],
  address_type smallint default 4,
  date_from timestamp with time zone NOT NULL default NOW(),
  date_to timestamp with time zone NOT NULL default NOW(),
  ts timestamp with time zone NOT NULL default NOW()
);
CREATE INDEX link_asn0_idx ON links (asn0);
CREATE INDEX link_asn1_idx ON links (asn1);