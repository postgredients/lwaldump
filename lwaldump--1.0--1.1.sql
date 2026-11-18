-- complain if script is sourced in psql, rather than via ALTER EXTENSION UPDATE
\echo Use "ALTER EXTENSION lwaldump UPDATE" to load this file. \quit

CREATE OR REPLACE FUNCTION lwaldump_with_timeline()
    RETURNS TABLE(timeline integer, flush_lsn pg_lsn)
    AS 'MODULE_PATHNAME'
    LANGUAGE C;
