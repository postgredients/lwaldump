-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION lw_waldump" to load this file. \quit

CREATE FUNCTION lw_waldump()
    RETURNS pg_lsn
    AS 'MODULE_PATHNAME'
    LANGUAGE C;
