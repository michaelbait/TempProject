-- LINKS TABLE
DROP TABLE IF EXISTS links;
CREATE TABLE links
(
    id           serial                      NOT NULL PRIMARY KEY,
    an0          character varying,
    an1          character varying,
    rank0        integer                              default 999999999,
    rank1        integer                              default 999999999,
    rank         integer                              default 999999999,
    number_paths bigint                               default 0,
    relationship character varying,

    ip_version   integer                              default 4,

    asn0         json,
    asn1         json,
    asn0_cone    json,
    asn1_cone    json,
    locations    json,
    corrected_by json,

    ts           timestamp without time zone NOT NULL default LOCALTIMESTAMP(2),
    date         date                        not null default CURRENT_DATE
);
CREATE INDEX link_an0_idx ON links (an0);
CREATE INDEX link_an1_idx ON links (an1);
CREATE INDEX link_rel_idx ON links (relationship);
CREATE INDEX link_rank_idx ON links (rank);