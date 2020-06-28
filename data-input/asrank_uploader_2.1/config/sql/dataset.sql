DROP TABLE IF EXISTS dataset;
CREATE TABLE dataset
(
    id                        serial                      NOT NULL PRIMARY KEY,
    dataset_id                character varying(256),
    ip_version                integer                              default 4,
    number_addresses          bigint                               default 0,
    number_prefixes           bigint                               default 0,
    number_asns               bigint                               default 0,
    number_asns_seen          bigint                               default 0,
    number_organizations      bigint                               default 0,
    number_organizations_seen bigint                               default 0,

    country                   json,
    clique                    json,
    asn_ixs                   json,
    sources                   json,
    asn_reserved_ranges       json,
    asn_assigned_ranges       json,

    ts                        timestamp without time zone NOT NULL default LOCALTIMESTAMP(2),
    date                      date                        not null default CURRENT_DATE
);
CREATE INDEX dataset_id_idx ON dataset (dataset_id);