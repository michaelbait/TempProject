-- PREFIXES TABLE
DROP TABLE IF EXISTS prefixes;
CREATE TABLE prefixes
(
    id         serial                      NOT NULL PRIMARY KEY,
    asn        bigint,
    network    text[],
    length     BIGINT,

    ip_version integer                              default 4,

    origin     json,

    ts         timestamp without time zone NOT NULL default LOCALTIMESTAMP(2),
    date       date                        not null default CURRENT_DATE
);
