-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION lwaldump" to load this file. \quit

CREATE FUNCTION lwaldump()
    RETURNS pg_lsn
    AS 'MODULE_PATHNAME'
    LANGUAGE C;
