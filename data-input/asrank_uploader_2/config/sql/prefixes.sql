-- PREFIXES TABLE
DROP TABLE IF EXISTS prefixes;
CREATE TABLE prefixes(
  id serial NOT NULL PRIMARY KEY,
  asn bigint,
  network text[],
  length BIGINT,
  origin json,
  range json,
  valid_date_first timestamp with time zone NOT NULL default NOW(),
  valid_date_last timestamp with time zone NOT NULL default NOW()
);
