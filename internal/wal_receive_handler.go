package internal

import (
	"context"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"regexp"
	"time"
)

const (
	// Sets standbyMessageTimeout in Streaming Replication Protocol.
	streamTimeout = 10
)
/*
Things to test:
* running without slot
* Can we prevent / detect a wal-gap
* If a slot already exists as logical slot
* Multiple versions of postgres
* Different wal size (>=pg11)
* timeline increase

Things to do:
* unittests for queryrunner code
* upgrade to pgx/v4
* use pglogrepl.LSN in replace internal/wal_segment_no
* public / private classes and functions (first case on names)
* proper sizes for int's
*/

type genericWalReceiveError struct {
	error
}

func (err genericWalReceiveError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// HandleWALReceive is invoked to receive wal with a replication connection and push
func HandleWALReceive(uploader *WalUploader) {
	// NOTE: Preventing a WAL gap is a complex one (also not 100% fixed with arch_command).
	// * Using replication slot helps, but that should be created and maintained
	//   by wal-g on standby's too (making sure unconsumed wals are preserved on
	//   potential new masters too)
	// * Using sync replication is another option, but non-promotable, and we
	//   should locally cache to disonnect S3 performance from database performance
	// Lets focus on creating wal files from repl msg first...

	// Connect to postgres.
	var XLogPos pglogrepl.LSN
	var segment *WalSegment

	slot, walSegmentBytes, err := getCurrentWalInfo()
	tracelog.ErrorLogger.FatalOnError(err)

	conn, err := pgconn.Connect(context.Background(), "replication=yes")
	tracelog.ErrorLogger.FatalOnError(err)
	defer conn.Close(context.Background())

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	tracelog.ErrorLogger.FatalOnError(err)

	if slot.Exists {
		XLogPos = slot.RestartLSN
	} else {
		tracelog.InfoLogger.Println("Trying to create the replication slot")
		_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slot.Name, "",
			pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.PhysicalReplication})
		tracelog.ErrorLogger.FatalOnError(err)
		XLogPos = sysident.XLogPos
	}

	// Get timeline for XLogPos from historyfile with helper function
	timeline, err := getStartTimeline(conn, uploader, sysident.Timeline, XLogPos)
	tracelog.ErrorLogger.FatalOnError(err)

	segment = NewWalSegment(timeline, XLogPos, walSegmentBytes)
	startReplication(conn, segment, slot.Name)
	for {
		streamResult, err := segment.Stream(conn, time.Second * streamTimeout)
		tracelog.ErrorLogger.FatalOnError(err)
		tracelog.DebugLogger.Printf("Succesfully received wal segment %s: ", segment.Name())

		switch streamResult {
		case ProcessMessageOK:
			// segment is a regular segemnt. Write, and create a new for this timeline.
			err = uploader.UploadWalFile(newNamedReaderImpl(segment, segment.Name()))
			tracelog.ErrorLogger.FatalOnError(err)
			XLogPos = segment.endLSN
			segment, err = segment.NextWalSegment()
			tracelog.ErrorLogger.FatalOnError(err)
		case ProcessMessageCopyDone:
			// segment is a partial. Write, and create a new for the next timeline.
			timeline++
			timelinehistfile, err := pglogrepl.TimelineHistory(context.Background(), conn, timeline)
			tracelog.ErrorLogger.FatalOnError(err)
			tlh, err := NewTimeLineHistFile(timeline, timelinehistfile.FileName, timelinehistfile.Content)
			tracelog.ErrorLogger.FatalOnError(err)
			err = uploader.UploadWalFile(newNamedReaderImpl(tlh, tlh.Name()))
			tracelog.ErrorLogger.FatalOnError(err)
			segment = NewWalSegment(timeline, XLogPos, walSegmentBytes)
			startReplication(conn, segment, slot.Name)
		default:
			tracelog.ErrorLogger.FatalOnError(errors.Errorf("Unexpected result from WalSegment.Stream() %v", streamResult))
		}
	}
}

func getStartTimeline(conn *pgconn.PgConn, uploader *WalUploader, systemTimeline int32, xLogPos pglogrepl.LSN)  (int32, error){
	if systemTimeline < 2 {
		return 1, nil
	}
	timelinehistfile, err := pglogrepl.TimelineHistory(context.Background(), conn, systemTimeline)
	if err == nil {
		tlh, err := NewTimeLineHistFile(systemTimeline, timelinehistfile.FileName, timelinehistfile.Content)
		tracelog.ErrorLogger.FatalOnError(err)
		err = uploader.UploadWalFile(newNamedReaderImpl(tlh, tlh.Name()))
		tracelog.ErrorLogger.FatalOnError(err)
		return tlh.LSNToTimeLine(xLogPos)
	}
	if pgErr, ok := err.(*pgconn.PgError); ok {
		if pgErr.Code == "58P01" {
			return systemTimeline, nil
		}
	}
	return 0, nil
}

func startReplication(conn *pgconn.PgConn, segment *WalSegment, slotName string) {
	tracelog.DebugLogger.Printf("Starting replication from %s: ", segment.StartLSN)
	err := pglogrepl.StartReplication(context.Background(), conn, slotName, segment.StartLSN,
		pglogrepl.StartReplicationOptions{Timeline: segment.TimeLine, Mode: pglogrepl.PhysicalReplication})
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Println("Started replication")
}

//  validateSlotName validates pgSlotName to be a valid slot name
func validateSlotName(pgSlotName string) (err error){
	// Check WALG_SLOTNAME env variable (can be any of [0-9A-Za-z_], and 1-63 characters long)
	invalid, err := regexp.MatchString(`\W`, pgSlotName)
	if err != nil {
		return
	}
	if len(pgSlotName) > 63 || invalid {
		err = genericWalReceiveError{errors.Errorf("%s can only contain 1-63 word characters ([0-9A-Za-z_])", PgSlotName)}
	}
	return
}

func getCurrentWalInfo() (slot PhysicalSlot, walSegmentBytes uint64, err error) {
	slotName := GetPgSlotName()

	// Creating a temporary connection to read slot info and wal_segment_size
	tmpConn, err := Connect()
	if err != nil {
		return
	}
	defer tmpConn.Close()

	queryRunner, err := newPgQueryRunner(tmpConn)
	if err != nil {
		return
	}

	slot, err = queryRunner.GetPhysicalSlotInfo(slotName)
	if err != nil {
		return
	}

	walSegmentBytes, err = queryRunner.GetWalSegmentBytes()
	return
}
