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

type SegmentError struct {
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
	lastMsg         *pgproto3.BackendMessage
}

type ProcessMessageResult int

const (
	ProcessMessageOK ProcessMessageResult = iota
	ProcessMessageUnknown
	ProcessMessageCopyDone
	ProcessMessageReplyRequested
	ProcessMessageSegmentGap
	ProcessMessageMismatch
)

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

// Create next wal file from result of Stream
func (seg *WalSegment) NextWalSegment() (*WalSegment, error) {
	// Next on this timeline, but read rest of msg
	if ! seg.IsComplete() {
		return nil, SegmentError{
			errors.Errorf("Cannot run NextWalSegment until IsComplete")}
	}
	nextSegment := NewWalSegment(seg.TimeLine, seg.endLSN, seg.walSegmentBytes)
	if seg.lastMsg != nil {
		result, err := nextSegment.ProcessMessage(*seg.lastMsg)
		if err != nil {
			return nil, err
		}
		if result != ProcessMessageOK {
			return nil, SegmentError{
				errors.Errorf("Unexpected result from ProcessMessage in NextWalSegment")}
		}
	}
	return nextSegment, nil
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

func (seg *WalSegment) ProcessMessage(message pgproto3.BackendMessage) (ProcessMessageResult, error) {
	var messageOffset pglogrepl.LSN
	switch msg := message.(type) {
	case *pgproto3.CopyData:
		switch msg.Data[0] {
		case pglogrepl.PrimaryKeepaliveMessageByteID:
			pkm, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
			tracelog.ErrorLogger.FatalOnError(err)
			tracelog.DebugLogger.Println("Primary Keepalive Message =>", "ServerWALEnd:", pkm.ServerWALEnd, "ServerTime:", pkm.ServerTime, "ReplyRequested:", pkm.ReplyRequested)

			if pkm.ReplyRequested {
				return ProcessMessageReplyRequested, nil
			}
		case pglogrepl.XLogDataByteID:
			xld, err := pglogrepl.ParseXLogData(msg.Data[1:])
			tracelog.ErrorLogger.FatalOnError(err)
			if xld.WALStart > seg.endLSN {
				// This message started after this segment ended
				return ProcessMessageMismatch, SegmentError{
					errors.Errorf("Message mismatch: Message started after end of this segment")}
			}
			walEnd := pglogrepl.LSN(uint64(xld.WALStart) + uint64(len(xld.WALData)))
			if walEnd < seg.StartLSN {
				// This message ended before this segment started
				return ProcessMessageMismatch, SegmentError{
					errors.Errorf("Message mismatch: Message ended before start of this segment")}
			}
			if xld.WALStart < seg.StartLSN {
				// This message started before this segment started, but should still have a piece for this segment
				messageOffset = seg.StartLSN - xld.WALStart
			}
			tracelog.DebugLogger.Println("XLogData =>", "WALStart", xld.WALStart, "WALEnd", walEnd,
				"LenWALData", len(string(xld.WALData)), "ServerWALEnd", xld.ServerWALEnd, "messageOffset",  messageOffset)//, "ServerTime:", xld.ServerTime)
			if seg.StartLSN + pglogrepl.LSN(seg.writeIndex) != (xld.WALStart + messageOffset) {
				return ProcessMessageSegmentGap, SegmentError{
					errors.Errorf("WAL segment error: CopyData WALStart does not fit to segment writeIndex")}
			}
			copiedBytes := copy(seg.data[seg.writeIndex:], xld.WALData[messageOffset:])
			seg.writeIndex += copiedBytes
			if copiedBytes < len(xld.WALData[messageOffset:]) {
				seg.lastMsg = &message
			}
		}
	case *pgproto3.CopyDone:
		return ProcessMessageCopyDone, nil
	default:
		return ProcessMessageUnknown,SegmentError {errors.Errorf("Received unexpected message: %#v\n", msg)}
	}
	return ProcessMessageOK, nil
}

func (seg *WalSegment) Stream(conn *pgconn.PgConn, standbyMessageTimeout time.Duration) (ProcessMessageResult, error) {
	// Inspired by https://github.com/jackc/pglogrepl/blob/master/example/pglogrepl_demo/main.go
	// And https://www.postgresql.org/docs/12/protocol-replication.html

	var err error
	var msg pgproto3.BackendMessage
	nextStandbyMessageDeadline := time.Now()
	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: seg.StartLSN})
			tracelog.ErrorLogger.FatalOnError(err)
			tracelog.DebugLogger.Println("Sent Standby status message")
			nextStandbyMessageDeadline = time.Now().Add(standbyMessageTimeout)
		}

		ctx, cancel := context.WithDeadline(context.Background(), nextStandbyMessageDeadline)
		msg, err = conn.ReceiveMessage(ctx)
		cancel()
		if pgconn.Timeout(err) {
			continue
		}
		tracelog.ErrorLogger.FatalOnError(err)

		result, err := seg.ProcessMessage(msg)
		switch result {
		case ProcessMessageOK:
			if seg.IsComplete() {
				return ProcessMessageOK, nil
			}
		case ProcessMessageUnknown:
			return result, err
		case ProcessMessageCopyDone:
			cdr, err := pglogrepl.SendStandbyCopyDone(context.Background(), conn)
			tracelog.ErrorLogger.FatalOnError(err)
			tracelog.DebugLogger.Printf("CopyDoneResult => %v", cdr)
			return result, nil
		case ProcessMessageReplyRequested:
			if seg.IsComplete() {
				return ProcessMessageOK, nil
			}
			nextStandbyMessageDeadline = time.Time{}
		case ProcessMessageSegmentGap:
			return result, err
		case ProcessMessageMismatch:
			return result, err
		default:
			tracelog.DebugLogger.Printf("Unexpected ProcessMessage result => %v", result)
			return result, err
		}
	}
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

