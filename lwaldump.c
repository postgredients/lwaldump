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

#include <unistd.h>

#include "access/xlogreader.h"
#include "access/xlogrecord.h"
#include "access/htup_details.h"
#include "access/xlog_internal.h"
#include "access/xlog.h"
#include "access/rmgr.h"
#include "access/timeline.h"
#if PG_VERSION_NUM >= 150000
#include "access/xlogrecovery.h"
#endif
#include "access/transam.h"
#include "catalog/pg_control.h"
#include "miscadmin.h"
#include "replication/walreceiver.h"

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
	XLogSegNo	current_file_segno;
	XLogRecPtr	received_upto;
	TimeLineID	received_tli;
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

/* A recycled future segment still has the old segment's long header. */
static bool
segment_has_current_wal(int fd, XLogSegNo segno, TimeLineID tli)
{
	PGAlignedXLogBlock buf;
	XLogLongPageHeader longhdr = (XLogLongPageHeader) buf.data;
	static const char zero_header[sizeof(XLogLongPageHeaderData)] = {0};
	char		fname[MAXPGPATH];
	ssize_t		nread;
	uint32		offset;
	uint32		i;

	XLogFileName(fname, tli, segno, WalSegSz);
	nread = pg_pread(fd, buf.data, sizeof(XLogLongPageHeaderData), 0);
	if (nread < 0)
		elog(ERROR, "could not read WAL segment header %s: %m", fname);
	if (nread != sizeof(XLogLongPageHeaderData))
		elog(ERROR, "short WAL segment header in %s", fname);

	if (memcmp(buf.data, zero_header, sizeof(zero_header)) == 0)
	{
		/* A preallocated empty segment is entirely zero, not just its header. */
		for (offset = 0; offset < WalSegSz; offset += XLOG_BLCKSZ)
		{
			CHECK_FOR_INTERRUPTS();
			nread = pg_pread(fd, buf.data, XLOG_BLCKSZ, offset);
			if (nread != XLOG_BLCKSZ)
				elog(ERROR, "could not inspect empty WAL segment %s at offset %u",
					 fname, offset);
			for (i = 0; i < XLOG_BLCKSZ; i++)
				if (buf.data[i] != 0)
					elog(ERROR, "nonzero data after zero WAL segment header in %s",
						 fname);
		}
		return false;
	}
	if (longhdr->std.xlp_magic != XLOG_PAGE_MAGIC ||
		!(longhdr->std.xlp_info & XLP_LONG_HEADER))
		elog(ERROR, "invalid WAL segment header in %s", fname);
	if (longhdr->xlp_sysid != GetSystemIdentifier())
		elog(ERROR, "WAL file is from different database system: %s", fname);
	if (longhdr->xlp_seg_size != WalSegSz ||
		longhdr->xlp_xlog_blcksz != XLOG_BLCKSZ)
		elog(ERROR, "invalid WAL segment size or block size in %s", fname);
	if (longhdr->std.xlp_pageaddr != segno * WalSegSz)
		return false;

	return true;
}

/* Use the running server's WAL directory and segment size. */
static void
identify_target_directory(XLogDumpPrivate *private)
{
	private->inpath = psprintf("%s/%s", DataDir, XLOGDIR);
	WalSegSz = wal_segment_size;
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
		/* A segment name can match even after this history entry ended. */
		if (entry->end != InvalidXLogRecPtr &&
			entry->end < private->startptr)
			continue;

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
			bool		usable;

			PG_TRY();
			{
				usable = segment_has_current_wal(fd, segno, entry->tli);
			}
			PG_CATCH();
			{
				close(fd);
				PG_RE_THROW();
			}
			PG_END_TRY();
			if (!usable)
			{
				close(fd);
				continue;
			}

			*file_tli = entry->tli;
			if (file_history != NULL)
				*file_history = entry;
			return fd;
		}
	}

	return -1;
}

/* WAL received on an ancestor after its fork is not on the selected path. */
static XLogRecPtr
received_on_path_end(XLogDumpPrivate *private)
{
	XLogRecPtr	switchpoint;

	if (private->received_tli == 0 ||
		!tliInHistory(private->received_tli, private->history))
		return InvalidXLogRecPtr;

	switchpoint = tliSwitchPoint(private->received_tli,
									private->history, NULL);
	if (switchpoint != InvalidXLogRecPtr &&
		switchpoint < private->received_upto)
		return switchpoint;
	return private->received_upto;
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
		if (file_history->end != InvalidXLogRecPtr &&
			file_history->end < private->startptr)
			elog(ERROR, "could not find WAL at the replay position");
		last_segno = segno;
		found = true;

		/* Do not scan an ancestor's copy past its switchpoint. */
		if (file_history->end != InvalidXLogRecPtr &&
			file_history->end < (segno + 1) * WalSegSz)
			return file_history->end;
	}

	if (!found)
	{
		if (XLogSegmentOffset(private->startptr, WalSegSz) != 0 ||
			received_on_path_end(private) > private->startptr)
			elog(ERROR, "could not find WAL at the replay position");
		return private->startptr;
	}

	return (last_segno + 1) * WalSegSz;
}

static bool
timeline_contains_point(TimeLineID timeline, XLogRecPtr lsn, List *history)
{
	XLogRecPtr	switchpoint;

	if (!tliInHistory(timeline, history))
		return false;

	switchpoint = tliSwitchPoint(timeline, history, NULL);
	return switchpoint == InvalidXLogRecPtr || switchpoint >= lsn;
}

/*
 * Infer "latest" conservatively.  The WAL receiver's timeline is only an
 * ancestor that PostgreSQL has streamed; it does not expose recoveryTargetTLI,
 * which belongs to the startup process.  Reject histories for which different
 * arrival orders could leave PostgreSQL on different recovery targets.
 */
static TimeLineID
find_latest_recovery_target(TimeLineID replay_tli, XLogRecPtr replay_lsn)
{
	TimeLineID	anchor_tli = replay_tli;
	TimeLineID	receive_tli = 0;
	TimeLineID	selected_tli;
	TimeLineID	newest_tli;
	TimeLineID	probe_tli;
	XLogRecPtr	receive_lsn;
	bool		newest_is_on_anchor_path = true;
	List	   *history;

	receive_lsn = GetWalRcvFlushRecPtr(NULL, &receive_tli);
	if (receive_tli != 0 && receive_lsn >= replay_lsn)
	{
		history = readTimeLineHistory(receive_tli);
		if (timeline_contains_point(replay_tli, replay_lsn, history))
			anchor_tli = receive_tli;
		list_free_deep(history);
	}

	selected_tli = anchor_tli;
	newest_tli = findNewestTimeLine(anchor_tli);
	/* Compare every candidate with the fixed timeline known to be reachable. */
	for (probe_tli = anchor_tli + 1; probe_tli <= newest_tli; probe_tli++)
	{
		bool		on_anchor_path;

		history = readTimeLineHistory(probe_tli);
		on_anchor_path = timeline_contains_point(anchor_tli, replay_lsn,
											 history);
		if (probe_tli == newest_tli)
			newest_is_on_anchor_path = on_anchor_path;

		if (on_anchor_path)
		{
			if (selected_tli == anchor_tli ||
				timeline_contains_point(selected_tli, replay_lsn, history))
				selected_tli = probe_tli;
			else
				elog(ERROR, "could not determine recovery path: timelines %u and %u are sibling descendants of timeline %u",
					 selected_tli, probe_tli, anchor_tli);
		}
		list_free_deep(history);
	}

	if (selected_tli != anchor_tli && !newest_is_on_anchor_path)
		elog(ERROR, "could not determine recovery path: timeline %u descends from timeline %u, but newest timeline %u does not",
			 selected_tli, anchor_tli, newest_tli);

	return selected_tli;
}

/* Resolve the recovery target from PostgreSQL's parsed postmaster GUC state. */
static TimeLineID
find_recovery_target_tli(TimeLineID replay_tli, XLogRecPtr replay_lsn,
						 bool *target_is_latest)
{
	*target_is_latest = false;

	switch (recoveryTargetTimeLineGoal)
	{
		case RECOVERY_TARGET_TIMELINE_CONTROLFILE:
			return replay_tli;
		case RECOVERY_TARGET_TIMELINE_LATEST:
			*target_is_latest = true;
			return find_latest_recovery_target(replay_tli, replay_lsn);
		case RECOVERY_TARGET_TIMELINE_NUMERIC:
			return recoveryTargetTLIRequested;
	}

	elog(ERROR, "unrecognized recovery target timeline goal: %d",
		 (int) recoveryTargetTimeLineGoal);
	return 0;
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
	private->current_file_segno = nextSegNo;
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
	if (count >= sizeof(XLogPageHeaderData))
	{
		XLogPageHeader header = (XLogPageHeader) readBuff;

		/* Recovery rejects valid WAL pages from outside its history. */
		if (header->xlp_magic == XLOG_PAGE_MAGIC &&
			!tliInHistory(header->xlp_tli, private->history))
			elog(ERROR, "WAL page at %X/%X belongs to timeline %u outside the recovery path",
				 (uint32) (targetPagePtr >> 32), (uint32) targetPagePtr,
				 header->xlp_tli);
	}

	return count;
}

/*
 * A zero end-of-WAL marker is expected in the last written page.  Check that
 * it is followed only by empty or recycled pages, not more current WAL.
 * A nonzero partial record is not a safe endpoint after a crash.
 */
static bool
unwritten_tail_after(XLogReaderState *state, XLogRecPtr last_lsn)
{
	XLogDumpPrivate *private = state->private_data;
	XLogSegNo	segno;
	uint32		segoff;
	uint32		pageoff;
	uint32		page_start;
	PGAlignedXLogBlock buf;
	ssize_t		nread;
	uint32		off;

	XLByteToSeg(last_lsn, segno, WalSegSz);
	/* XLogReaderInvalReadState() clears ws_segno on an invalid record. */
	if (state->seg.ws_file < 0 || private->current_file_segno != segno)
		return false;

	segoff = XLogSegmentOffset(last_lsn, WalSegSz);
	pageoff = segoff % XLOG_BLCKSZ;
	page_start = segoff - pageoff;
	if (pageoff != 0)
	{
		nread = pg_pread(state->seg.ws_file, buf.data, XLOG_BLCKSZ,
						 page_start);
		if (nread != XLOG_BLCKSZ)
			elog(ERROR, "could not inspect WAL tail at %X/%X",
				 (uint32) (last_lsn >> 32), (uint32) last_lsn);
		for (off = MAXALIGN(pageoff); off < XLOG_BLCKSZ; off++)
			if (buf.data[off] != 0)
				return false;
		page_start += XLOG_BLCKSZ;
	}

	for (; page_start < WalSegSz;
		 page_start += XLOG_BLCKSZ)
	{
		XLogPageHeader header;
		static const char zero_header[sizeof(XLogPageHeaderData)] = {0};

		nread = pg_pread(state->seg.ws_file, buf.data,
						 sizeof(XLogPageHeaderData), page_start);
		if (nread != sizeof(XLogPageHeaderData))
			elog(ERROR, "could not inspect WAL tail at offset %u", page_start);
		header = (XLogPageHeader) buf.data;
		if (header->xlp_pageaddr == segno * WalSegSz + page_start)
			return false;
		if (memcmp(buf.data, zero_header, sizeof(zero_header)) == 0)
		{
			nread = pg_pread(state->seg.ws_file, buf.data,
							 XLOG_BLCKSZ, page_start);
			if (nread != XLOG_BLCKSZ)
				elog(ERROR, "could not inspect WAL tail at offset %u", page_start);
			for (off = sizeof(zero_header); off < XLOG_BLCKSZ; off++)
				if (buf.data[off] != 0)
					return false;
		}
	}

	return true;
}

/* A promotion record can start in a page that still has its parent's TLI. */
static void
check_record_timeline(XLogReaderState *state, XLogRecord *record,
					  TimeLineID target_tli)
{
	TimeLineID	record_tli;
	uint8		info;

	if (record->xl_rmid != RM_XLOG_ID)
		return;

	info = record->xl_info & ~XLR_INFO_MASK;
	if (info == XLOG_END_OF_RECOVERY)
	{
		xl_end_of_recovery end_of_recovery;

		if (XLogRecGetDataLen(state) < sizeof(end_of_recovery))
			elog(ERROR, "short end-of-recovery WAL record at %X/%X",
				 (uint32) (state->ReadRecPtr >> 32),
				 (uint32) state->ReadRecPtr);
		memcpy(&end_of_recovery, XLogRecGetData(state),
			   sizeof(end_of_recovery));
		record_tli = end_of_recovery.ThisTimeLineID;
	}
	else if (info == XLOG_CHECKPOINT_SHUTDOWN ||
			 info == XLOG_CHECKPOINT_ONLINE)
	{
		CheckPoint	checkpoint;

		if (XLogRecGetDataLen(state) < sizeof(checkpoint))
			elog(ERROR, "short checkpoint WAL record at %X/%X",
				 (uint32) (state->ReadRecPtr >> 32),
				 (uint32) state->ReadRecPtr);
		memcpy(&checkpoint, XLogRecGetData(state), sizeof(checkpoint));
		record_tli = checkpoint.ThisTimeLineID;
	}
	else
		return;

	if (record_tli != target_tli)
		elog(ERROR, "WAL record at %X/%X belongs to timeline %u outside the recovery path",
			 (uint32) (state->ReadRecPtr >> 32),
			 (uint32) state->ReadRecPtr, record_tli);
}


/* Read the selected local path through its last complete WAL record. */
static XLogRecPtr
scan_local_wal(XLogDumpPrivate *private, TimeLineID replay_tli,
			   TimeLineID *last_tli)
{
	XLogRecPtr	last_lsn;
	XLogReaderState *xlogreader_state;
	XLogRecord *record;
	char	   *errormsg;

	if (private->endptr <= private->startptr)
	{
		if (private->endptr < private->startptr)
			elog(ERROR, "could not find WAL at the replay position");
		if (received_on_path_end(private) > private->startptr)
			elog(ERROR, "local WAL ends at the replay position before the known received position");
		*last_tli = replay_tli;
		return private->startptr;
	}

	xlogreader_state =
        XLogReaderAllocate(WalSegSz, private->inpath,
                           XL_ROUTINE(.page_read = WALDumpReadPage,
                                      .segment_open = WALDumpOpenSegment,
                                      .segment_close = WALDumpCloseSegment),
						   private);
	if (!xlogreader_state)
		elog(ERROR, "out of memory");

	PG_TRY();
	{
		xlogreader_state->system_identifier = GetSystemIdentifier();
		XLogBeginRead(xlogreader_state, private->startptr);
		last_lsn = private->startptr;
		*last_tli = replay_tli;
		for (;;)
		{
			XLogSegNo	last_segno;
			XLogSegNo	end_segno;

			record = XLogReadRecord(xlogreader_state, &errormsg);
			if (!record)
			{
				XLByteToSeg(last_lsn, last_segno, WalSegSz);
				end_segno = private->endptr / WalSegSz;
				/* XLogReader identifies a record whose continuation was lost. */
				if (private->endptr_reached &&
					!XLogRecPtrIsInvalid(xlogreader_state->abortedRecPtr))
					elog(ERROR, "WAL record at %X/%X extends beyond available local WAL",
						 (uint32) (xlogreader_state->abortedRecPtr >> 32),
						 (uint32) xlogreader_state->abortedRecPtr);
				if (errormsg != NULL && !private->endptr_reached &&
					(last_segno + 1 < end_segno ||
					 !unwritten_tail_after(xlogreader_state, last_lsn)))
					elog(ERROR, "could not scan WAL after %X/%X: %s",
						 (uint32) (last_lsn >> 32), (uint32) last_lsn,
						 errormsg);
				if (received_on_path_end(private) > last_lsn)
					elog(ERROR, "local WAL ends at %X/%X before the known received position %X/%X",
						 (uint32) (last_lsn >> 32), (uint32) last_lsn,
						 (uint32) (private->received_upto >> 32),
						 (uint32) private->received_upto);
				break;
			}
			check_record_timeline(xlogreader_state, record, replay_tli);
			last_lsn = xlogreader_state->EndRecPtr;
			*last_tli = tliOfPointInHistory(xlogreader_state->ReadRecPtr,
											 private->history);
		}
	}
	PG_CATCH();
	{
		XLogReaderFree(xlogreader_state);
		PG_RE_THROW();
	}
	PG_END_TRY();
	XLogReaderFree(xlogreader_state);
	return last_lsn;
}

static XLogRecPtr
lwaldump_scan(TimeLineID *timeline)
{
	XLogRecPtr	last_lsn;
	XLogDumpPrivate private;
	XLogRecPtr	switchpoint;
	TimeLineID	replay_tli;
	TimeLineID	target_tli;
	TimeLineID	last_tli;
	TimeLineID	final_replay_tli;
	TimeLineID	final_received_tli;
	XLogRecPtr	final_replay_lsn;
	XLogRecPtr	final_received_upto;
	bool		target_is_latest;

	memset(&private, 0, sizeof(XLogDumpPrivate));
	private.received_upto = InvalidXLogRecPtr;

	if (GetRecoveryPauseState() != RECOVERY_PAUSED)
		elog(ERROR, "WAL replay must be paused before a local WAL scan");
	if (WalRcvRunning())
		elog(ERROR, "WAL receiver must be stopped before a local WAL scan");
	if (recoveryTarget != RECOVERY_TARGET_UNSET)
		elog(ERROR, "a point-in-time recovery target prevents replay of the local WAL endpoint");
	private.received_upto = GetWalRcvFlushRecPtr(NULL,
											&private.received_tli);

	identify_target_directory(&private);
	private.startptr = GetXLogReplayRecPtr(&replay_tli);
	if (XLogRecPtrIsInvalid(private.startptr))
		elog(ERROR, "no replay WAL location available");

	target_tli = find_recovery_target_tli(replay_tli, private.startptr,
									  &target_is_latest);
	private.history = readTimeLineHistory(target_tli);
	if (!tliInHistory(replay_tli, private.history))
		elog(ERROR, "replay timeline %u is not in target timeline %u history",
			 replay_tli, target_tli);

	switchpoint = tliSwitchPoint(replay_tli, private.history, NULL);
	if (switchpoint != InvalidXLogRecPtr && switchpoint < private.startptr)
		elog(ERROR, "target timeline %u forked from replay timeline %u before the replay position",
			 target_tli, replay_tli);

	/*
	 * The startup process can retain an open ancestor segment that is no
	 * longer visible in pg_wal.  A SQL backend cannot inspect that read
	 * state, so local child WAL does not prove which branch replay will take.
	 */
	if (target_tli != replay_tli)
		elog(ERROR, "could not determine recovery path: replay has not reached target timeline %u",
			 target_tli);

	private.endptr = find_local_wal_end(&private);
	last_lsn = scan_local_wal(&private, replay_tli, &last_tli);
	final_replay_lsn = GetXLogReplayRecPtr(&final_replay_tli);
	final_received_upto = GetWalRcvFlushRecPtr(NULL, &final_received_tli);
	if (GetRecoveryPauseState() != RECOVERY_PAUSED || WalRcvRunning() ||
		final_replay_lsn != private.startptr ||
		final_replay_tli != replay_tli ||
		final_received_upto != private.received_upto ||
		final_received_tli != private.received_tli)
		elog(ERROR, "WAL recovery state changed during local WAL scan");

	if (target_is_latest &&
		find_latest_recovery_target(replay_tli, private.startptr) != target_tli)
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
