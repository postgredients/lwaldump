/*-------------------------------------------------------------------------
 *
 * lwaldump.c
 *     example extenstion for PostgreSQL
 *
 * Copyright (c) 2014-2023, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		lwaldump.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"
#include "fmgr.h"
#include "funcapi.h"
#include "utils/builtins.h"
#include "utils/guc.h"
#include "utils/pg_lsn.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/htup_details.h"
#include "access/xlog_internal.h"
#include "access/xlog.h"
#include "access/timeline.h"
#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif
#include "access/transam.h"
#include "miscadmin.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(lwaldump);
PG_FUNCTION_INFO_V1(lwaldump_with_timeline);

static int	WalSegSz;

typedef struct XLogDumpPrivate
{
	char	   *inpath;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
	bool		endptr_reached;
	List	   *history;
	TimeLineID	current_file_tli;
} XLogDumpPrivate;


/*
 * Open the file in the valid target directory.
 *
 * return a read only fd
 */
static int
open_file_in_directory(const char *directory, const char *fname)
{
	int			fd = -1;
	char		fpath[MAXPGPATH];
	char * errormsg;

	Assert(directory != NULL);

	snprintf(fpath, MAXPGPATH, "%s/%s", directory, fname);
	fd = open(fpath, O_RDONLY | PG_BINARY, 0);

	if (fd < 0 && errno != ENOENT) {
		errormsg = strerror(errno);
		elog(ERROR, "could not open file \"%s\": %s",
					fname, errormsg);
	}
	return fd;
}

/*
 * Try to find fname in the given directory. Returns true if it is found,
 * false otherwise. If fname is NULL, search the complete directory for any
 * file with a valid WAL file name. If file is successfully opened, set the
 * wal segment size.
 */
static bool
search_directory(const char *directory)
{
	int			fd = -1;
	DIR		   *xldir;
	char *errormsg;
	char *fname;

	/*
	 * A valid file name is not passed, so search the complete directory.  If
	 * we find any file whose name is a valid WAL file name then try to open
	 * it.  If we cannot open it, bail out.
	 */
	if ((xldir = opendir(directory)) != NULL)
	{
		struct dirent *xlde;

		while ((xlde = readdir(xldir)) != NULL)
		{
			if (IsXLogFileName(xlde->d_name))
			{
				fd = open_file_in_directory(directory, xlde->d_name);
				fname = xlde->d_name;
				break;
			}
		}

		closedir(xldir);
	}

	/* set WalSegSz if file is successfully opened */
	if (fd >= 0)
	{
		PGAlignedXLogBlock buf;
		int			r;

		r = read(fd, buf.data, XLOG_BLCKSZ);
		if (r == XLOG_BLCKSZ)
		{
			XLogLongPageHeader longhdr = (XLogLongPageHeader) buf.data;

			WalSegSz = longhdr->xlp_seg_size;

			if (!IsValidWalSegSize(WalSegSz))
				elog(ERROR, ngettext("WAL segment size must be a power of two between 1 MB and 1 GB, but the WAL file \"%s\" header specifies %d byte",
									 "WAL segment size must be a power of two between 1 MB and 1 GB, but the WAL file \"%s\" header specifies %d bytes",
									 WalSegSz),
							fname, WalSegSz);
		}
		else
		{
			if (errno != 0) {
				errormsg = strerror(errno);
				elog(ERROR, "could not read file \"%s\": %s",
							fname, errormsg);
			} else
				elog(ERROR, "could not read file \"%s\": read %d of %zu",
							fname, r, (Size) XLOG_BLCKSZ);
		}
		close(fd);
		return true;
	}

	return false;
}

/*
 * Identify the target directory and set WalSegSz.
 *
 * Try to find the file in several places:
 * if directory != NULL:
 *	 directory /
 *	 directory / XLOGDIR /
 * else
 *	 .
 *	 XLOGDIR /
 *	 $PGDATA / XLOGDIR /
 *
 * Set the valid target directory in private->inpath.
 */
static void
identify_target_directory(XLogDumpPrivate *private)
{
	char		fpath[MAXPGPATH];
	const char *datadir;

	/* current directory */
	if (search_directory("."))
	{
		private->inpath = strdup(".");
		return;
	}
	/* XLOGDIR */
	if (search_directory(XLOGDIR))
	{
		private->inpath = strdup(XLOGDIR);
		return;
	}

	datadir = getenv("PGDATA");
	/* $PGDATA / XLOGDIR */
	if (datadir != NULL)
	{
		snprintf(fpath, MAXPGPATH, "%s/%s", datadir, XLOGDIR);
		if (search_directory(fpath))
		{
			private->inpath = strdup(fpath);
			return;
		}
	}

	/* could not locate WAL file */
	elog(ERROR, "could not find any WAL file");
}

/* Open the newest usable local copy of a segment on the recovery path. */
static int
open_segment_from_history(XLogDumpPrivate *private, XLogSegNo segno,
						  TimeLineID min_tli, TimeLineID *file_tli,
						  TimeLineHistoryEntry **file_history)
{
	ListCell   *cell;

	foreach(cell, private->history)
	{
		TimeLineHistoryEntry *entry = (TimeLineHistoryEntry *) lfirst(cell);
		char		fname[MAXPGPATH];
		int			fd;

		if (entry->tli < min_tli)
			break;

		if (entry->begin != InvalidXLogRecPtr)
		{
			XLogSegNo	beginseg;

			XLByteToSeg(entry->begin, beginseg, WalSegSz);
			if (segno < beginseg)
				continue;
		}
		if (entry->end != InvalidXLogRecPtr)
		{
			XLogSegNo	endseg;

			XLByteToSeg(entry->end, endseg, WalSegSz);
			if (segno > endseg)
				continue;
		}

		XLogFileName(fname, entry->tli, segno, WalSegSz);
		fd = open_file_in_directory(private->inpath, fname);
		if (fd >= 0)
		{
			*file_tli = entry->tli;
			if (file_history != NULL)
				*file_history = entry;
			return fd;
		}
	}

	return -1;
}

/*
 * Freeze the scan at the first segment absent from the local recovery path.
 * A later segment does not make a gap replayable.
 */
static XLogRecPtr
find_local_wal_end(XLogDumpPrivate *private)
{
	XLogSegNo	segno;
	XLogSegNo	last_segno = 0;
	TimeLineID	file_tli = 0;
	bool		found = false;

	XLByteToSeg(private->startptr, segno, WalSegSz);
	for (;; segno++)
	{
		int			fd;
		TimeLineHistoryEntry *file_history;

		fd = open_segment_from_history(private, segno, file_tli, &file_tli,
									   &file_history);
		if (fd < 0)
			break;

		close(fd);
		last_segno = segno;
		found = true;

		/* Do not scan an ancestor's copy past its switchpoint. */
		if (file_history->end != InvalidXLogRecPtr &&
			file_history->end <= (segno + 1) * WalSegSz)
			return file_history->end;
	}

	if (!found)
		return private->startptr;

	return (last_segno + 1) * WalSegSz;
}

/* Resolve the recovery target visible to this SQL backend. */
static TimeLineID
find_recovery_target_tli(TimeLineID replay_tli, bool *target_is_latest)
{
	const char *target;
	char	   *endptr;
	unsigned long parsed;

	target = GetConfigOption("recovery_target_timeline", false, false);
	*target_is_latest = false;

	if (strcmp(target, "current") == 0)
		return replay_tli;
	if (strcmp(target, "latest") == 0)
	{
		*target_is_latest = true;
		return findNewestTimeLine(replay_tli);
	}

	errno = 0;
	parsed = strtoul(target, &endptr, 10);
	if (errno != 0 || *endptr != '\0' || parsed == 0 ||
		parsed > (unsigned long) ((TimeLineID) -1))
		elog(ERROR, "invalid recovery target timeline: \"%s\"", target);

	return (TimeLineID) parsed;
}

/* lwaldump's XLogReaderRoutine->segment_open callback */
static void
WALDumpOpenSegment(XLogReaderState *state, XLogSegNo nextSegNo,
				   TimeLineID *tli_p)
{
	XLogDumpPrivate *private = state->private_data;

	state->seg.ws_file = open_segment_from_history(private, nextSegNo,
												  private->current_file_tli, tli_p,
												  NULL);
	if (state->seg.ws_file < 0)
		elog(ERROR, "could not find WAL segment %llu on the recovery path",
			 (unsigned long long) nextSegNo);

	private->current_file_tli = *tli_p;
}

/*
 * lwaldump's XLogReaderRoutine->segment_close callback.  Same as
 * wal_segment_close
 */
static void
WALDumpCloseSegment(XLogReaderState *state)
{
	close(state->seg.ws_file);
	/* need to check errno? */
	state->seg.ws_file = -1;
}

/* lwaldump's XLogReaderRoutine->page_read callback */
static int
WALDumpReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				XLogRecPtr targetPtr, char *readBuff)
{
	XLogDumpPrivate *private = state->private_data;
	int			count = XLOG_BLCKSZ;
	WALReadError errinfo;

	if (private->endptr != InvalidXLogRecPtr)
	{
		if (targetPagePtr + XLOG_BLCKSZ <= private->endptr)
			count = XLOG_BLCKSZ;
		else if (targetPagePtr + reqLen <= private->endptr)
			count = private->endptr - targetPagePtr;
		else
		{
			private->endptr_reached = true;
			return -1;
		}
	}

	if (!WALRead(state, readBuff, targetPagePtr, count,
				 private->current_file_tli,
				 &errinfo))
	{
		WALOpenSegment *seg = &errinfo.wre_seg;
		char		fname[MAXPGPATH];

		XLogFileName(fname, seg->ws_tli, seg->ws_segno,
					 state->segcxt.ws_segsize);

		if (errinfo.wre_errno != 0)
		{
			errno = errinfo.wre_errno;
			elog(ERROR, "could not read from file %s, offset %u: %m",
						fname, errinfo.wre_off);
		}
		else
			elog(ERROR, "could not read from file %s, offset %u: read %d of %zu",
						fname, errinfo.wre_off, errinfo.wre_read,
						(Size) errinfo.wre_req);
	}

	return count;
}


static XLogRecPtr
lwaldump_scan(TimeLineID *timeline)
{
	XLogRecPtr	last_lsn;
	XLogReaderState *xlogreader_state;
	XLogDumpPrivate private;
	XLogRecord *record;
	XLogRecPtr	first_record;
	XLogRecPtr	switchpoint;
	TimeLineID	replay_tli;
	TimeLineID	target_tli;
	TimeLineID	last_tli;
	bool		target_is_latest;
	char	   *errormsg;

	memset(&private, 0, sizeof(XLogDumpPrivate));

	private.startptr = InvalidXLogRecPtr;
	private.endptr = InvalidXLogRecPtr;
	private.endptr_reached = false;
	private.history = NIL;
	private.current_file_tli = 0;

	identify_target_directory(&private);
	private.startptr = GetXLogReplayRecPtr(&replay_tli);
	if (XLogRecPtrIsInvalid(private.startptr))
		elog(ERROR, "no replay WAL location available");

	target_tli = find_recovery_target_tli(replay_tli, &target_is_latest);
	private.history = readTimeLineHistory(target_tli);
	if (!tliInHistory(replay_tli, private.history))
		elog(ERROR, "replay timeline %u is not in target timeline %u history",
			 replay_tli, target_tli);

	switchpoint = tliSwitchPoint(replay_tli, private.history, NULL);
	if (switchpoint != InvalidXLogRecPtr && switchpoint < private.startptr)
		elog(ERROR, "target timeline %u forked from replay timeline %u before the replay position",
			 target_tli, replay_tli);

	private.endptr = find_local_wal_end(&private);

	/* done with argument parsing, do the actual work */

	/* we have everything we need, start reading */
	xlogreader_state =
        XLogReaderAllocate(WalSegSz, private.inpath,
                           XL_ROUTINE(.page_read = WALDumpReadPage,
                                      .segment_open = WALDumpOpenSegment,
                                      .segment_close = WALDumpCloseSegment),
                           &private);
	if (!xlogreader_state)
		elog(ERROR, "out of memory");


	first_record = private.startptr;
	XLogBeginRead(xlogreader_state, first_record);

	last_lsn = private.startptr;
	last_tli = replay_tli;
	/*
	 * Display a message that we're skipping data if `from` wasn't a pointer
	 * to the start of a record and also wasn't a pointer to the beginning of
	 * a segment (e.g. we were used in file mode).
	 */
	if (first_record != private.startptr &&
		XLogSegmentOffset(private.startptr, WalSegSz) != 0) {
		elog(LOG, ngettext("first record is after %X/%X, at %X/%X, skipping over %u byte\n",
						"first record is after %X/%X, at %X/%X, skipping over %u bytes\n",
						(first_record - private.startptr)),
			   (uint32) (private.startptr >> 32), (uint32) private.startptr,
			   (uint32) (first_record >> 32), (uint32) first_record,
			   (uint32) (first_record - private.startptr));
	} else {
		elog(LOG, "first record is after %X/%X, at %X/%X",
		(uint32) (private.startptr >> 32), (uint32) private.startptr,
		(uint32) (first_record >> 32), (uint32) first_record);
	}

	for (;;)
	{
		/* try to read the next record */
		record = XLogReadRecord(xlogreader_state, &errormsg);
		if (!record)
		{
			break;
		}
		/* after reading the first record, continue at next one */
		first_record = InvalidXLogRecPtr;
		last_lsn = xlogreader_state->EndRecPtr;
		last_tli = tliOfPointInHistory(xlogreader_state->ReadRecPtr,
										 private.history);

		elog(LOG, "next record is at %X/%X\n", (uint32) (last_lsn >> 32), (uint32) last_lsn);
	}


	XLogReaderFree(xlogreader_state);

	if (target_is_latest && findNewestTimeLine(replay_tli) != target_tli)
		elog(ERROR, "new recovery target timeline appeared during WAL scan");

	*timeline = last_tli;
	return last_lsn;
}

Datum
lwaldump(PG_FUNCTION_ARGS)
{
	TimeLineID	timeline;

	if (!RecoveryInProgress())
		elog(ERROR, "do not run lwaldump on primary");

	PG_RETURN_LSN(lwaldump_scan(&timeline));
}

Datum
lwaldump_with_timeline(PG_FUNCTION_ARGS)
{
	TimeLineID	timeline;
	XLogRecPtr	last_lsn;
	TupleDesc	tupdesc;
	HeapTuple	tuple;
	Datum		values[2];
	bool		nulls[2] = {false, false};

	if (!RecoveryInProgress())
		elog(ERROR, "do not run lwaldump on primary");

	last_lsn = lwaldump_scan(&timeline);
	if (get_call_result_type(fcinfo, NULL, &tupdesc) != TYPEFUNC_COMPOSITE)
		ereport(ERROR,
				(errcode(ERRCODE_FEATURE_NOT_SUPPORTED),
				 errmsg("lwaldump_with_timeline must be called in a record context")));

	values[0] = Int32GetDatum(timeline);
	values[1] = LSNGetDatum(last_lsn);
	tuple = heap_form_tuple(BlessTupleDesc(tupdesc), values, nulls);
	PG_RETURN_DATUM(HeapTupleGetDatum(tuple));
}
