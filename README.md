# lw_waldump
Tool for getting last LSN flushed to disk by replica(required in clusters with quorum_commit)

Usage on primary:
CREATE EXTENSION lw_waldump;

Usage on standby:
SELECT lw_waldump(); 
