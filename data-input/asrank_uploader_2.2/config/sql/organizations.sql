-- ORGS TABLE
DROP TABLE IF EXISTS organizations;
CREATE TABLE organizations
(
    id         serial                      NOT NULL PRIMARY KEY,
    org_id     character varying,
    org_name   character varying,
    rank       bigint                               default 0,
    seen       boolean                              default true,
    source     character varying,

    ip_version integer                              default 4,

    asns       json,
    cone       json,
    country    json,
    members    json,
    asndegree  json,
    orgdegree  json,
    announcing json,

    ts         timestamp without time zone NOT NULL default LOCALTIMESTAMP(2),
    date       date                        not null default CURRENT_DATE
);
CREATE INDEX orgs_org_idx ON organizations (org_id);
CREATE INDEX orgs_name_idx ON organizations (org_name);