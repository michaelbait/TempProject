-- CONES TABLE
DROP TABLE IF EXISTS cones;
CREATE TABLE cones
(
    id   serial                      NOT NULL PRIMARY KEY,
    aid  bigint,
    rank bigint,
    asn  json,
    cone json,
    asns json,
    pfx  json,

    ts   timestamp without time zone NOT NULL default LOCALTIMESTAMP(2),
    date date                        not null default CURRENT_DATE
);
CREATE INDEX cones_asn_idx ON cones (aid);
