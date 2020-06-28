-- ASNS TABLE
DROP TABLE IF EXISTS asns;
CREATE TABLE asns
(
    id            serial                      NOT NULL PRIMARY KEY,
    asn           bigint,
    asn_name      character varying,
    org_id        character varying,
    org_name      character varying,
    rank          integer                              default 0,
    source        character varying,
    seen          boolean                              default false,
    ixp           boolean                              default false,
    clique_member boolean                              default false,

    longitude     float                                default 0.0,
    latitude      float                                default 0.0,

    country       json,
    cone          json,
    asndegree     json,
    announcing    json,

    ts            timestamp without time zone NOT NULL default LOCALTIMESTAMP(2),
    date          date                        not null default CURRENT_DATE
);
CREATE INDEX asns_asn_idx ON asns (asn);
CREATE INDEX asns_name_idx ON asns (asn_name);
CREATE INDEX asns_orgid_idx ON asns (org_id);
CREATE INDEX asns_orgname_idx ON asns (org_name);