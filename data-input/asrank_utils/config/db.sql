-- ASNS TABLE
DROP TABLE IF EXISTS asns;
CREATE TABLE asns(
  id serial NOT NULL PRIMARY KEY,
  asn character varying NOT NULL,
  asn_name character varying NOT NULL,
  source character varying NOT NULL,
  org_id character varying NOT NULL,
  org_name character varying NOT NULL,
  country character varying NOT NULL,
  country_name character varying NOT NULL,
  latitude float default 0.0,
  longitude float  default 0.0,
  rank integer default 0,
  customer_cone_asns integer default 0,
  customer_cone_prefixes integer default 0,
  customer_cone_addresses integer default 0,
  degree_peer integer default 0,
  degree_global integer default 0,
  degree_customer integer default 0,
  degree_sibling integer default 0,
  degree_transit integer default 0,
  degree_provider integer default 0,
  ts timestamp with time zone NOT NULL
);
CREATE INDEX asns_asn_idx ON public.asns (asn);
CREATE INDEX asns_asn_name_idx ON public.asns (asn_name);
CREATE INDEX asns_org_idx ON public.asns (org_id);
CREATE INDEX asns_org_name_idx ON public.asns (org_name);