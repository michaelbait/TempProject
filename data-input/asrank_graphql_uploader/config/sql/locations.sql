-- LOCATIONS TABLE
DROP TABLE IF EXISTS locations;
CREATE TABLE locations(
  id serial NOT NULL PRIMARY KEY,
  locid character varying default null,
  city character varying,
  country character varying,
  continent character varying,
  region character varying,
  population bigint default 0,
  longitude float default 0.0,
  latitude float default 0.0,
  range json,
  valid_date_first timestamp with time zone NOT NULL default NOW(),
  valid_date_last timestamp with time zone NOT NULL default NOW()
);
CREATE INDEX loc_city_idx ON locations (city);
CREATE INDEX loc_country_idx ON locations (country);