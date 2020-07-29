/*
This object represents data about a physical slot.
The data can be retrieved from Postgres with the queryRunner,
and is consumed by the walReceiveHandler.
*/
package internal

import (
	"context"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"io"
	"time"
)

type SegmentFloodError struct {
	error
}

type SegmentGapError struct {
	error
}

// The PhysicalSlot represents a Physical Replication Slot.
type WalSegment struct {
	TimeLine        int32
	StartLSN        pglogrepl.LSN
	endLSN          pglogrepl.LSN
	walSegmentBytes uint64
	data            []byte
	readIndex       int
	writeIndex      int
}

func NewWalSegment(timeline int32, location pglogrepl.LSN, walSegmentBytes uint64) *WalSegment {
	//We could test validity of walSegmentBytes (not implemented):
	//  https://www.postgresql.org/docs/11/app-initdb.html:
	//    Set the WAL segment size, in megabytes...
	//    The value must be a power of 2 between 1 and 1024 (megabytes)

	segment := &WalSegment{TimeLine: timeline, walSegmentBytes: walSegmentBytes}
	// Calculate start byte of file from location (which could be anywhere in this file)
	segment.StartLSN = pglogrepl.LSN((uint64(location) / walSegmentBytes) * walSegmentBytes)
	// Calculate end form start and number of bytes in this file
	segment.endLSN = segment.StartLSN + pglogrepl.LSN(walSegmentBytes)
	// Allocate data
	segment.data = make([]byte, walSegmentBytes)
	return segment
}

// Name returns the filename of this wal segment
func (seg *WalSegment) Name() string {
	// '0/2A33FE00' -> '00000001000000000000002A'
	segId := uint64(seg.StartLSN)/uint64(seg.walSegmentBytes)
	if seg.IsComplete() {
		return fmt.Sprintf("%08X%016X", seg.TimeLine, segId)
	}
	return fmt.Sprintf("%08X%016X.partial", seg.TimeLine, segId)
}

func (seg *WalSegment) Stream(conn *pgconn.PgConn, standbyMessageTimeout time.Duration) (bool, error) {
	// Inspired by https://github.com/jackc/pglogrepl/blob/master/example/pglogrepl_demo/main.go
	// And https://www.postgresql.org/docs/12/protocol-replication.html

	var err error
	nextStandbyMessageDeadline := time.Now()
	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: seg.StartLSN})
			tracelog.ErrorLogger.FatalOnError(err)
			tracelog.DebugLogger.Println("Sent Standby status message")
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		msg, err := conn.ReceiveMessage(ctx)
		cancel()
		if pgconn.Timeout(err) {
			continue
		}
		tracelog.ErrorLogger.FatalOnError(err)

		switch msg := msg.(type) {
		case *pgproto3.CopyData:
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				tracelog.ErrorLogger.FatalOnError(err)
				tracelog.DebugLogger.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

				if pkm.ReplyRequested {
					nextStandbyMessageDeadline = time.Time{}
				}

			case pglogrepl.XLogDataByteID:
				xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
				tracelog.ErrorLogger.FatalOnError(err)
				walEnd := pglogrepl.LSN(uint64(xld.WALStart) + uint64(len(xld.WALData)))
				tracelog.DebugLogger.Println("XLogData =>", "WALStart", xld.WALStart, "WALEnd", walEnd,
					"LenWALData", len(string(xld.WALData)), "ServerWALEnd", xld.ServerWALEnd) //, "ServerTime:", xld.ServerTime)
				if seg.StartLSN + pglogrepl.LSN(seg.writeIndex) != xld.WALStart {
					return seg.IsComplete(), SegmentGapError{
						errors.Errorf("WAL segment error: CopyData WALStart does not fit to segment writeIndex")}
				}
				copiedBytes := copy(seg.data[seg.writeIndex:], xld.WALData)
				seg.writeIndex += copiedBytes
				if copiedBytes < len(xld.WALData) {
					return seg.IsComplete(), SegmentFloodError{
						errors.Errorf("WAL segment error: CopyData does not fit in segment")}
				}
				if seg.IsComplete() {
					return true, nil
				}
			}
		case *pgproto3.CopyDone:
			cdr, err := pglogrepl.SendStandbyCopyDone(context.Background(), conn)
			tracelog.ErrorLogger.FatalOnError(err)
			tracelog.DebugLogger.Printf("CopyDoneResult => %e", cdr)
			return false, nil
		default:
			tracelog.DebugLogger.Printf("Received unexpected message: %#v\n", msg)
		}
	}
	return false, errors.Errorf("Ended outside of loop")
}

// IsComplete returns true when all data is added
func (seg *WalSegment) IsComplete() bool {
	if seg.StartLSN + pglogrepl.LSN(seg.writeIndex) >= seg.endLSN {
		return true
	}
	return false
}

// Read is what makes the WalSegment a io.Reader, which can be handled by WalUploader.UploadWalFile
func (seg *WalSegment) Read(p []byte) (n int, err error) {
	n = copy(p, seg.data[seg.readIndex:])
	seg.readIndex += n
	if len(seg.data) <= seg.readIndex {
		return n, io.EOF
	}
	return n, nil
}

