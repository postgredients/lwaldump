# lwaldump
Tool for getting last LSN flushed to disk by replica(required in clusters with quorum_commit)

Usage on primary:
CREATE EXTENSION lwaldump;

Usage on standby:
SELECT lwaldump(); 
