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
#include "utils/pg_lsn.h"
#include "utils/guc.h"

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
	TimeLineID	timeline;
	TimeLineHistoryEntry *history;
	int			history_count;
	char	   *inpath;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
	bool		endptr_reached;
} XLogDumpPrivate;

/* Read a history file from pg_wal only; PostgreSQL's helper may restore one. */
static bool
read_local_history(const char *directory, TimeLineID target,
				   TimeLineHistoryEntry **entries, int *count)
{
	char		name[MAXFNAMELEN];
	char		path[MAXPGPATH];
	char		line[1024];
	FILE	   *file;
	TimeLineHistoryEntry *result = NULL;
	int			n = 0;
	int			capacity = 0;
	XLogRecPtr	begin = InvalidXLogRecPtr;
	TimeLineID	previous = 0;

	TLHistoryFileName(name, target);
	snprintf(path, sizeof(path), "%s/%s", directory, name);
	file = fopen(path, "r");
	if (file == NULL)
	{
		if (errno == ENOENT)
			return false;
		elog(ERROR, "could not open timeline history file \"%s\": %m", path);
	}

	while (fgets(line, sizeof(line), file) != NULL)
	{
		unsigned int parent, hi, lo;
		XLogRecPtr switchpoint;

		if (line[0] == '#' || line[0] == '\n')
			continue;
		if (sscanf(line, "%u\t%X/%X", &parent, &hi, &lo) != 3)
			elog(ERROR, "invalid timeline history file \"%s\"", path);
		switchpoint = ((uint64) hi << 32) | lo;
		if (parent <= previous || parent >= target || switchpoint <= begin)
			elog(ERROR, "inconsistent timeline history file \"%s\"", path);
		if (n == capacity)
		{
			capacity = capacity ? capacity * 2 : 4;
			result = result ? repalloc(result, capacity * sizeof(*result)) : palloc(capacity * sizeof(*result));
		}
		result[n].tli = parent;
		result[n].begin = begin;
		result[n].end = switchpoint;
		n++;
		previous = parent;
		begin = switchpoint;
	}
	if (ferror(file))
		elog(ERROR, "could not read timeline history file \"%s\": %m", path);
	fclose(file);
	if (n == 0)
		elog(ERROR, "empty timeline history file \"%s\"", path);
	result = repalloc(result, (n + 1) * sizeof(*result));
	result[n].tli = target;
	result[n].begin = begin;
	result[n].end = InvalidXLogRecPtr;
	*entries = result;
	*count = n + 1;
	return true;
}


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

/*
 * Freeze the scan at the end of the newest local segment on this timeline.
 * Without an upper bound XLogReader tries to open the next, absent segment
 * after consuming a segment exactly to its end.
 */
static XLogRecPtr
find_local_wal_end(const char *directory, TimeLineID timeline)
{
	DIR		   *xldir;
	struct dirent *xlde;
	XLogSegNo	max_segno = 0;
	bool		found = false;

	xldir = opendir(directory);
	if (xldir == NULL)
		elog(ERROR, "could not open directory \"%s\": %m", directory);

	while ((xlde = readdir(xldir)) != NULL)
	{
		TimeLineID	file_timeline;
		XLogSegNo	segno;

		if (!IsXLogFileName(xlde->d_name))
			continue;

		XLogFromFileName(xlde->d_name, &file_timeline, &segno, WalSegSz);
		if (file_timeline == timeline && (!found || segno > max_segno))
		{
			max_segno = segno;
			found = true;
		}
	}

	closedir(xldir);
	if (!found)
		elog(ERROR, "could not find a WAL file for timeline %u", timeline);

	return (max_segno + 1) * WalSegSz;
}

static bool
local_segment_exists(const char *directory, TimeLineID timeline,
					 XLogSegNo segno)
{
	char		name[MAXFNAMELEN];
	char		path[MAXPGPATH];

	XLogFileName(name, timeline, segno, WalSegSz);
	snprintf(path, sizeof(path), "%s/%s", directory, name);
	return access(path, F_OK) == 0;
}

static void
find_local_history(XLogDumpPrivate *private)
{
	DIR		   *dir;
	struct dirent *entry;
	TimeLineID	best = private->timeline;
	TimeLineID	replay_tli = private->timeline;
	TimeLineHistoryEntry *best_history = NULL;
	int			best_count = 0;
	const char *target_setting = GetConfigOption("recovery_target_timeline", false, false);
	TimeLineID	target_tli = 0;

	if (strcmp(target_setting, "current") == 0)
		return;
	if (strcmp(target_setting, "latest") != 0 &&
		(sscanf(target_setting, "%u", &target_tli) != 1 || target_tli == 0))
		elog(ERROR, "invalid recovery_target_timeline: %s", target_setting);

	dir = opendir(private->inpath);
	if (dir == NULL)
		elog(ERROR, "could not open directory \"%s\": %m", private->inpath);
	while ((entry = readdir(dir)) != NULL)
	{
		TimeLineID	candidate;
		TimeLineHistoryEntry *history;
		int			count;
		int			i;
		XLogSegNo	segno;

		if (!IsTLHistoryFileName(entry->d_name) ||
			sscanf(entry->d_name, "%8X", &candidate) != 1 || candidate <= best)
			continue;
		if (target_tli != 0 && candidate != target_tli)
			continue;
		if (!read_local_history(private->inpath, candidate, &history, &count))
			continue;
		for (i = 0; i < count; i++)
			if (history[i].tli == replay_tli &&
				private->startptr >= history[i].begin &&
				(history[i].end == InvalidXLogRecPtr || private->startptr <= history[i].end))
				break;
		if (i == count)
		{
			pfree(history);
			continue;
		}
		XLByteToSeg(history[count - 1].begin, segno, WalSegSz);
		if (!local_segment_exists(private->inpath, candidate, segno))
		{
			pfree(history);
			continue;
		}
		if (best_history)
			pfree(best_history);
		best_history = history;
		best_count = count;
		best = candidate;
	}
	closedir(dir);
	if (best_history)
	{
		private->history = best_history;
		private->history_count = best_count;
		private->endptr = find_local_wal_end(private->inpath, best);
	}
}

static TimeLineID
timeline_for_segment(XLogDumpPrivate *private, XLogRecPtr pageptr)
{
	XLogSegNo	segno;
	XLogRecPtr	segment_end;
	int			i;

	if (!private->history)
		return private->timeline;
	XLByteToSeg(pageptr, segno, WalSegSz);
	segment_end = (segno + 1) * WalSegSz;
	for (i = private->history_count - 1; i >= 0; i--)
		if (private->history[i].begin < segment_end)
			return private->history[i].tli;
	return private->history[0].tli;
}

static TimeLineID
timeline_for_record(XLogDumpPrivate *private, XLogRecPtr recptr)
{
	int			i;

	if (!private->history)
		return private->timeline;
	for (i = private->history_count - 1; i >= 0; i--)
		if (private->history[i].begin <= recptr)
			return private->history[i].tli;
	return private->history[0].tli;
}

/* lwaldump's XLogReaderRoutine->segment_open callback */
static void
WALDumpOpenSegment(XLogReaderState *state, XLogSegNo nextSegNo,
				   TimeLineID *tli_p)
{
	TimeLineID	tli = *tli_p;
	char		fname[MAXPGPATH];
	int			tries;

	XLogFileName(fname, tli, nextSegNo, state->segcxt.ws_segsize);

	/*
	 * In follow mode there is a short period of time after the server has
	 * written the end of the previous file before the new file is available.
	 * So we loop for 5 seconds looking for the file to appear before giving
	 * up.
	 */
	for (tries = 0; tries < 10; tries++)
	{
		state->seg.ws_file = open_file_in_directory(state->segcxt.ws_dir, fname);
		if (state->seg.ws_file >= 0)
			return;
		if (errno == ENOENT)
		{
			int			save_errno = errno;

			/* File not there yet, try again */
			pg_usleep(500 * 1000);

			errno = save_errno;
			continue;
		}
		/* Any other error, fall through and fail */
		break;
	}

	elog(ERROR, "could not find file \"%s\": %m", fname);
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
	TimeLineID	file_timeline;
	XLogSegNo	segno;

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

	file_timeline = timeline_for_segment(private, targetPagePtr);
	XLByteToSeg(targetPagePtr, segno, WalSegSz);
	if (!local_segment_exists(private->inpath, file_timeline, segno))
	{
		private->endptr_reached = true;
		return -1;
	}
	if (!WALRead(state, readBuff, targetPagePtr, count, file_timeline,
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
	char	   *errormsg;

	memset(&private, 0, sizeof(XLogDumpPrivate));

	private.timeline = 1;
	private.startptr = InvalidXLogRecPtr;
	private.endptr = InvalidXLogRecPtr;
	private.endptr_reached = false;

	identify_target_directory(&private);
	private.startptr = GetXLogReplayRecPtr(&private.timeline);
	if (XLogRecPtrIsInvalid(private.startptr))
		elog(ERROR, "no replay WAL location available");
	private.endptr = find_local_wal_end(private.inpath, private.timeline);
	find_local_history(&private);

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
		private.timeline = timeline_for_record(&private, xlogreader_state->ReadRecPtr);

		elog(LOG, "next record is at %X/%X\n", (uint32) (last_lsn >> 32), (uint32) last_lsn);
	}


	XLogReaderFree(xlogreader_state);
	if (private.history)
		pfree(private.history);

	*timeline = private.timeline;
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
