CREATE USER metabase
WITH
    PASSWORD 'metabase';

CREATE DATABASE metabasedb;

ALTER DATABASE metabasedb OWNER TO metabase;
CREATE EXTENSION IF NOT EXISTS citext;

\c metabasedb
ALTER SCHEMA public OWNER TO metabase;