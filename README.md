# lw_waldump
Tool for getting last LSN flushed to disk by replica(required in clusters with quorum_commit)
Usage: lw_waldump -s <since_lsn> -p <path_to_pg_wal>
