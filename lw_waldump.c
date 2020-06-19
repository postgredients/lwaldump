/*-------------------------------------------------------------------------
 *
 * hello_ext.c
 *     example extenstion for PostgreSQL
 *
 * Copyright (c) 2014-2015, PostgreSQL Global Development Group
 *
 * IDENTIFICATION
 *		hello_ext/hello_ext.c
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

PG_FUNCTION_INFO_V1(lw_waldump);

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

typedef struct XLogDumpConfig
{
	/* display options */
	bool		bkp_details;
	int			stop_after_records;
	int			already_displayed_records;
	bool		follow;
	bool		stats;
	bool		stats_per_record;

	/* filter options */
	int			filter_by_rmgr;
	TransactionId filter_by_xid;
	bool		filter_by_xid_enabled;
} XLogDumpConfig;

typedef struct Stats
{
	uint64		count;
	uint64		rec_len;
	uint64		fpi_len;
} Stats;

#define MAX_XLINFO_TYPES 16
static void fatal_error(const char *fmt,...) pg_attribute_printf(1, 2);

/*
 * Big red button to push when things go horribly wrong.
 */
static void
fatal_error(const char *fmt,...)
{
	va_list		args;

	fflush(stdout);

	fprintf(stderr, _("%s: FATAL:  "), progname);
	va_start(args, fmt);
	vfprintf(stderr, _(fmt), args);
	va_end(args);
	fputc('\n', stderr);

	exit(EXIT_FAILURE);
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

	Assert(directory != NULL);

	snprintf(fpath, MAXPGPATH, "%s/%s", directory, fname);
	fd = open(fpath, O_RDONLY | PG_BINARY, 0);

	if (fd < 0 && errno != ENOENT)
		fatal_error("could not open file \"%s\": %s",
					fname, strerror(errno));
	return fd;
}

/*
 * Try to find fname in the given directory. Returns true if it is found,
 * false otherwise. If fname is NULL, search the complete directory for any
 * file with a valid WAL file name. If file is successfully opened, set the
 * wal segment size.
 */
static bool
search_directory(const char *directory, const char *fname)
{
	int			fd = -1;
	DIR		   *xldir;

	/* open file if valid filename is provided */
	if (fname != NULL)
		fd = open_file_in_directory(directory, fname);

	/*
	 * A valid file name is not passed, so search the complete directory.  If
	 * we find any file whose name is a valid WAL file name then try to open
	 * it.  If we cannot open it, bail out.
	 */
	else if ((xldir = opendir(directory)) != NULL)
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
				fatal_error(ngettext("WAL segment size must be a power of two between 1 MB and 1 GB, but the WAL file \"%s\" header specifies %d byte",
									 "WAL segment size must be a power of two between 1 MB and 1 GB, but the WAL file \"%s\" header specifies %d bytes",
									 WalSegSz),
							fname, WalSegSz);
		}
		else
		{
			if (errno != 0)
				fatal_error("could not read file \"%s\": %s",
							fname, strerror(errno));
			else
				fatal_error("could not read file \"%s\": read %d of %zu",
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
identify_target_directory(XLogDumpPrivate *private, char *directory,
						  char *fname)
{
	char		fpath[MAXPGPATH];

	if (directory != NULL)
	{
		if (search_directory(directory, fname))
		{
			private->inpath = strdup(directory);
			return;
		}

		/* directory / XLOGDIR */
		snprintf(fpath, MAXPGPATH, "%s/%s", directory, XLOGDIR);
		if (search_directory(fpath, fname))
		{
			private->inpath = strdup(fpath);
			return;
		}
	}
	else
	{
		const char *datadir;

		/* current directory */
		if (search_directory(".", fname))
		{
			private->inpath = strdup(".");
			return;
		}
		/* XLOGDIR */
		if (search_directory(XLOGDIR, fname))
		{
			private->inpath = strdup(XLOGDIR);
			return;
		}

		datadir = getenv("PGDATA");
		/* $PGDATA / XLOGDIR */
		if (datadir != NULL)
		{
			snprintf(fpath, MAXPGPATH, "%s/%s", datadir, XLOGDIR);
			if (search_directory(fpath, fname))
			{
				private->inpath = strdup(fpath);
				return;
			}
		}
	}

	/* could not locate WAL file */
	if (fname)
		fatal_error("could not locate WAL file \"%s\"", fname);
	else
		fatal_error("could not find any WAL file");
}

/*
 * Read count bytes from a segment file in the specified directory, for the
 * given timeline, containing the specified record pointer; store the data in
 * the passed buffer.
 */
static void
XLogDumpXLogRead(const char *directory, TimeLineID timeline_id,
				 XLogRecPtr startptr, char *buf, Size count)
{
	char	   *p;
	XLogRecPtr	recptr;
	Size		nbytes;

	static int	sendFile = -1;
	static XLogSegNo sendSegNo = 0;
	static uint32 sendOff = 0;

	p = buf;
	recptr = startptr;
	nbytes = count;

	while (nbytes > 0)
	{
		uint32		startoff;
		int			segbytes;
		int			readbytes;

		startoff = XLogSegmentOffset(recptr, WalSegSz);

		if (sendFile < 0 || !XLByteInSeg(recptr, sendSegNo, WalSegSz))
		{
			char		fname[MAXFNAMELEN];
			int			tries;

			/* Switch to another logfile segment */
			if (sendFile >= 0)
				close(sendFile);

			XLByteToSeg(recptr, sendSegNo, WalSegSz);

			XLogFileName(fname, timeline_id, sendSegNo, WalSegSz);

			/*
			 * In follow mode there is a short period of time after the server
			 * has written the end of the previous file before the new file is
			 * available. So we loop for 5 seconds looking for the file to
			 * appear before giving up.
			 */
			for (tries = 0; tries < 10; tries++)
			{
				sendFile = open_file_in_directory(directory, fname);
				if (sendFile >= 0)
					break;
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

			if (sendFile < 0)
				fatal_error("could not find file \"%s\": %s",
							fname, strerror(errno));
			sendOff = 0;
		}

		/* Need to seek in the file? */
		if (sendOff != startoff)
		{
			if (lseek(sendFile, (off_t) startoff, SEEK_SET) < 0)
			{
				int			err = errno;
				char		fname[MAXPGPATH];

				XLogFileName(fname, timeline_id, sendSegNo, WalSegSz);

				fatal_error("could not seek in log file %s to offset %u: %s",
							fname, startoff, strerror(err));
			}
			sendOff = startoff;
		}

		/* How many bytes are within this segment? */
		if (nbytes > (WalSegSz - startoff))
			segbytes = WalSegSz - startoff;
		else
			segbytes = nbytes;

		readbytes = read(sendFile, p, segbytes);
		if (readbytes <= 0)
		{
			int			err = errno;
			char		fname[MAXPGPATH];
			int			save_errno = errno;

			XLogFileName(fname, timeline_id, sendSegNo, WalSegSz);
			errno = save_errno;

			if (readbytes < 0)
				fatal_error("could not read from log file %s, offset %u, length %d: %s",
							fname, sendOff, segbytes, strerror(err));
			else if (readbytes == 0)
				fatal_error("could not read from log file %s, offset %u: read %d of %zu",
							fname, sendOff, readbytes, (Size) segbytes);
		}

		/* Update state for read */
		recptr += readbytes;

		sendOff += readbytes;
		nbytes -= readbytes;
		p += readbytes;
	}
}

/*
 * XLogReader read_page callback
 */
static int
XLogDumpReadPage(XLogReaderState *state, XLogRecPtr targetPagePtr, int reqLen,
				 XLogRecPtr targetPtr, char *readBuff, TimeLineID *curFileTLI)
{
	XLogDumpPrivate *private = state->private_data;
	int			count = XLOG_BLCKSZ;

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

	XLogDumpXLogRead(private->inpath, private->timeline, targetPagePtr,
					 readBuff, count);

	return count;
}

Datum
lw_waldump(PG_FUNCTION_ARGS)
{
	XLogRecPtr	last_lsn;
	XLogReaderState *xlogreader_state;
	XLogDumpPrivate private;
	XLogDumpConfig config;
	XLogRecord *record;
	XLogRecPtr	first_record;
	char	   *errormsg;

	memset(&private, 0, sizeof(XLogDumpPrivate));
	memset(&config, 0, sizeof(XLogDumpConfig));

	private.timeline = 1;
	private.startptr = InvalidXLogRecPtr;
	private.endptr = InvalidXLogRecPtr;
	private.endptr_reached = false;

	config.bkp_details = false;
	config.stop_after_records = -1;
	config.already_displayed_records = 0;
	config.follow = false;
	config.filter_by_rmgr = -1;
	config.filter_by_xid = InvalidTransactionId;
	config.filter_by_xid_enabled = false;
	config.stats = false;
	config.stats_per_record = false;

	identify_target_directory(&private, private.inpath, NULL);
	private.startptr = GetXLogReplayRecPtr(&private.timeline);
	/* we don't know what to print */
	if (XLogRecPtrIsInvalid(private.startptr))
	{
		fprintf(stderr, _("replayptr: %lu, timeline: %u\n"), private.startptr, private.timeline);
		fprintf(stderr, _("%s: no start WAL location given\n"), progname);
		goto bad_argument;
	}

	/* done with argument parsing, do the actual work */

	/* we have everything we need, start reading */
	xlogreader_state = XLogReaderAllocate(WalSegSz, XLogDumpReadPage,
										  &private);
	if (!xlogreader_state)
		fatal_error("out of memory");


	first_record = private.startptr;

	last_lsn = private.startptr;
	/*
	 * Display a message that we're skipping data if `from` wasn't a pointer
	 * to the start of a record and also wasn't a pointer to the beginning of
	 * a segment (e.g. we were used in file mode).
	 */
	if (first_record != private.startptr &&
		XLogSegmentOffset(private.startptr, WalSegSz) != 0)
		printf(ngettext("first record is after %X/%X, at %X/%X, skipping over %u byte\n",
						"first record is after %X/%X, at %X/%X, skipping over %u bytes\n",
						(first_record - private.startptr)),
			   (uint32) (private.startptr >> 32), (uint32) private.startptr,
			   (uint32) (first_record >> 32), (uint32) first_record,
			   (uint32) (first_record - private.startptr));

	for (;;)
	{
		/* try to read the next record */
		record = XLogReadRecord(xlogreader_state, first_record, &errormsg);
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
bad_argument:
	fprintf(stderr, _("Try \"%s --help\" for more information.\n"), progname);
	fatal_error("bad argument");
	PG_RETURN_LSN(last_lsn);
}
