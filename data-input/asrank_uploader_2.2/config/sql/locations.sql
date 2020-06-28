-- LOCATIONS TABLE
DROP TABLE IF EXISTS locations;
CREATE TABLE locations
(
    id         serial                      NOT NULL PRIMARY KEY,
    locid      character varying                    default null,
    city       character varying,
    country    character varying,
    continent  character varying,
    region     character varying,
    population bigint                               default 0,
    longitude  float                                default 0.0,
    latitude   float                                default 0.0,

    ip_version integer                              default 4,

    ts         timestamp without time zone NOT NULL default LOCALTIMESTAMP(2),
    date       date                        not null default CURRENT_DATE
);
CREATE INDEX loc_city_idx ON locations (city);
CREATE INDEX loc_country_idx ON locations (country);