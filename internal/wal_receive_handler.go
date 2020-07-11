package internal

import (
	"context"
	"fmt"
	"github.com/jackc/pgconn"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgproto3/v2"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"os"
	"regexp"
	"time"
)

/*
Things to test:
* running without slot
* Can we prevent / detect a wal-gap
* If a slot already exists as logical slot
* Multiple versions of postgres
* Different wal size (>=pg11)

Things to do:
* unittests for queryrunner code
* upgrade to pgx/v4
* use pglogrepl.LSN in replace internal/wal_segment_no
* public / private classes and functions (first case on names)
*/

type GenericWalReceiveError struct {
	error
}

func newGenericWalReceiveError(errortext string) GenericWalReceiveError {
	return GenericWalReceiveError{errors.Errorf("WAL receiver error: %s", errortext)}
}

func (err GenericWalReceiveError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// HandleWALReceive is invoked to receive wal with a replication connection and push
func HandleWALReceive(uploader *Uploader) {
	// NOTE: Preventing a WAL gap is a complex one (also not 100% fixed with arch_command).
	// * Using replication slot helps, but that should be created and maintained
	//   by wal-g on standby's too (making sure unconsumed wals are preserved on
	//   potential new masters too)
	// * Using sync replication is another option, but non-promotable, and we
	//   should locally cache to disonnect S3 performance from database performance
	// Lets focus on creating wal files from repl msg first...

	// Connect to postgres.
	var XLogPos        pglogrepl.LSN
	var walSegmentSize uint64
	var slotName       string

	// Check WALG_SLOTNAME env variable (can be any of [0-9A-Za-z_], and 1-63 characters long)
	slotNameRe := regexp.MustCompile(`\w{0,63}`)
	slotName = slotNameRe.FindString(os.Getenv("WALG_SLOTNAME"))
	if slotName == "" {
		tracelog.InfoLogger.Println("No (correct) replication slot specified. Using default name 'walg'.")
		slotName = "walg"
	} else {
		tracelog.InfoLogger.Printf("Using slotname %s: ", slotName)
	}

	// Creating a temporary connection to read slot info and wal_segment_size
	tracelog.DebugLogger.Println("Temp connection to read slot info")
	tmpConn, err := Connect()
	tracelog.ErrorLogger.FatalOnError(err)
	queryRunner, err := newPgQueryRunner(tmpConn)
	tracelog.ErrorLogger.FatalOnError(err)

	slot, err := queryRunner.GetPhysicalSlotInfo(slotName)
	tracelog.ErrorLogger.FatalOnError(err)

	walSegmentSize, err = queryRunner.GetWalSegmentSize()
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Printf("wal_segment_size = %s", walSegmentSize)

	err = tmpConn.Close()
	tracelog.ErrorLogger.FatalOnError(err)

	conn, err := pgconn.Connect(context.Background(), "replication=yes")
	tracelog.ErrorLogger.FatalOnError(err)
	defer conn.Close(context.Background())

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos.String(), "DBName:", sysident.DBName)

	if slot.Exists {
		XLogPos = slot.RestartLSN
	} else {
	  tracelog.InfoLogger.Println("Trying to create the replication slot")
		_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slot.Name, "",
																	           pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.PhysicalReplication})
		tracelog.ErrorLogger.FatalOnError(err)
	  tracelog.DebugLogger.Println("Replication slot created.")
		XLogPos = sysident.XLogPos
	}

  tracelog.DebugLogger.Printf("Starting replication from %s: ", XLogPos.String())
	err = pglogrepl.StartReplication(context.Background(), conn, slot.Name, XLogPos, pglogrepl.StartReplicationOptions{Timeline: sysident.Timeline, Mode: pglogrepl.PhysicalReplication})
	tracelog.ErrorLogger.FatalOnError(err)
  tracelog.DebugLogger.Println("Started replication")

	standbyMessageTimeout := time.Second * 10
	nextStandbyMessageDeadline := time.Now().Add(standbyMessageTimeout)

	// Inspired by https://github.com/jackc/pglogrepl/blob/master/example/pglogrepl_demo/main.go
	for {
		if time.Now().After(nextStandbyMessageDeadline) {
			err = pglogrepl.SendStandbyStatusUpdate(context.Background(), conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: XLogPos})
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
				tracelog.DebugLogger.Println("XLogData =>", "WALStart", xld.WALStart, "WALEnd", walEnd, "LenWALData", len(string(xld.WALData)), "ServerWALEnd", xld.ServerWALEnd) //, "ServerTime:", xld.ServerTime)

				XLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		default:
			tracelog.DebugLogger.Printf("Received unexpected message: %#v\n", msg)
		}
	}
}
