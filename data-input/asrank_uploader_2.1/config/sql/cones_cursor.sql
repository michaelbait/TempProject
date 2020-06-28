-- CONES_CURSOR TABLE
DROP TABLE IF EXISTS cones_cursor;
CREATE TABLE cones_cursor
(
    id   serial NOT NULL PRIMARY KEY,
    aid  bigint,
    date date   not null default CURRENT_DATE
);

CREATE INDEX cones_cursor_aid_idx ON cones_cursor USING btree (aid);
ALTER TABLE cones_cursor
    CLUSTER ON cones_cursor_aid_idx;

-- CONES_CURSOR TABLE
-- DROP TABLE IF EXISTS cc_test;
-- CREATE TABLE cc_test
-- (
--     id   serial NOT NULL PRIMARY KEY,
--     aid  bigint,
--     date date   not null default CURRENT_DATE
-- );
--
-- CREATE INDEX cc_test_aid_idx ON cc_test USING btree (aid);
-- ALTER TABLE cc_test
--     CLUSTER ON cc_test_aid_idx;