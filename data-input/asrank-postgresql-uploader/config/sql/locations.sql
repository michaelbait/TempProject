-- LOCATIONS TABLE
DROP TABLE IF EXISTS locations;
CREATE TABLE locations(
  id serial NOT NULL PRIMARY KEY,
  hid character varying default '512',
  lid character varying,
  city character varying,
  country character varying,
  continent character varying,
  region character varying,
  population bigint default 0,
  longitude float default 0.0,
  latitude float default 0.0,
  address_country character varying(10) default 'global',
  address_type smallint default 4,
  date_from timestamp with time zone NOT NULL default NOW(),
  date_to timestamp with time zone NOT NULL default NOW(),
  ts timestamp with time zone NOT NULL default NOW()
);
CREATE INDEX loc_lid_idx ON locations (lid);
CREATE INDEX loc_city_idx ON locations (city);