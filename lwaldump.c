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

#define FRONTEND 1

#include "postgres.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/pg_lsn.h"
#include "postgres.h"

#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>

#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/xlog_internal.h"
#include "access/xlog.h"
#include "access/transam.h"
#include "common/fe_memutils.h"
#include "common/logging.h"
#include "getopt_long.h"
#include "miscadmin.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(lwaldump);

static const char *progname;

static int	WalSegSz;

typedef struct XLogDumpPrivate
{
	TimeLineID	timeline;
	char	   *inpath;
	XLogRecPtr	startptr;
	XLogRecPtr	endptr;
	bool		endptr_reached;
} XLogDumpPrivate;


XLogRecord *
lwXLogReadRecord(XLogReaderState *state, char **errormsg);

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

	if (!WALRead(state, readBuff, targetPagePtr, count, private->timeline,
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


/*
 * Attempt to read an XLOG record.
 *
 * XLogBeginRead() or XLogFindNextRecord() must be called before the first call
 * to XLogReadRecord().
 *
 * If the page_read callback fails to read the requested data, NULL is
 * returned.  The callback is expected to have reported the error; errormsg
 * is set to NULL.
 *
 * If the reading fails for some other reason, NULL is also returned, and
 * *errormsg is set to a string with details of the failure.
 *
 * The returned pointer (or *errormsg) points to an internal buffer that's
 * valid until the next call to XLogReadRecord.
 */
XLogRecord *
lwXLogReadRecord(XLogReaderState *state, char **errormsg)
{
	DecodedXLogRecord *decoded;

	/*
	 * Release last returned record, if there is one.  We need to do this so
	 * that we can check for empty decode queue accurately.
	 */
	XLogReleasePreviousRecord(state);

	/*
	 * Call XLogReadAhead() in blocking mode to make sure there is something
	 * in the queue, though we don't use the result.
	 */
	// if (!XLogReaderHasQueuedRecordOrError(state))
	// 	XLogReadAhead(state, false /* nonblocking */ );

	/* Consume the head record or error. */
	decoded = XLogNextRecord(state, errormsg);
	if (decoded)
	{
		/*
		 * This function returns a pointer to the record's header, not the
		 * actual decoded record.  The caller will access the decoded record
		 * through the XLogRecGetXXX() macros, which reach the decoded
		 * recorded as xlogreader->record.
		 */
		Assert(state->record == decoded);
		return &decoded->header;
	}

	return NULL;
}


Datum
lwaldump(PG_FUNCTION_ARGS)
{
	XLogRecPtr	last_lsn;
	XLogReaderState *xlogreader_state;
	XLogDumpPrivate private;
	XLogRecord *record;
	XLogRecPtr	first_record;
	char	   *errormsg;

	if (!RecoveryInProgress()) {
		elog(ERROR, "do not run lwaldump on primary");
	}

	memset(&private, 0, sizeof(XLogDumpPrivate));

	private.timeline = 1;
	private.startptr = InvalidXLogRecPtr;
	private.endptr = InvalidXLogRecPtr;
	private.endptr_reached = false;

	identify_target_directory(&private);
	private.startptr = GetXLogReplayRecPtr(&private.timeline);
	/* we don't know what to print */
	if (XLogRecPtrIsInvalid(private.startptr))
	{
		elog(LOG, "replayptr: %lu, timeline: %u", private.startptr, private.timeline);
		elog(ERROR, "%s: no start WAL location given", progname);
	}

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
	xlogreader_state->EndRecPtr = first_record;

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
		record = lwXLogReadRecord(xlogreader_state, &errormsg);
		if (!record)
		{
			break;
		}
		/* after reading the first record, continue at next one */
		first_record = InvalidXLogRecPtr;
		last_lsn = xlogreader_state->EndRecPtr;
	}


	XLogReaderFree(xlogreader_state);

	PG_RETURN_LSN(last_lsn);
}
