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

// TODO : unit tests
// HandleWALReceive is invoked to receive wal with a replication connection and push
func HandleWALReceive(uploader *Uploader, tmpFilePath string) {
	// NOTE: Preventing a WAL gap is a complex one (also not 100% fixed with arch_command).
	// * Using replication slot helps, but that should be created and maintained
	//   by wal-g on standby's too (making sure unconsumed wals are preserved on
	//   potential new masters too)
	// * Using sync replication is another option, but non-promotable, and we
	//   should locally cache to disonnect S3 performance from database performance
	// Lets focus on creating wal files from repl msg first...

	// Create tmp dir (unless exists) --- Unknown if required...
	// Check you can create a file there
	// Unclear if this is required. Will get there, or clean up.

	// Connect to postgres.
	var XLogPos pglogrepl.LSN

	// Check WALG_SLOTNAME env variable (can be any of [0-9A-Za-z_], and 1-63 characters long)
	slotNameRe := regexp.MustCompile(`\w{0,63}`)
	slotName := slotNameRe.FindString(os.Getenv("WALG_SLOTNAME"))
	if slotName == "" {
		tracelog.InfoLogger.Println("No (correct) replication slot specified. Using default name 'walg'.")
		slotName = "walg"
	} else {
		tracelog.InfoLogger.Printf("Using slotname %s: ", slotName)
	}
	// Implement something that gets the timeline info from the slot.
	// Create temp conn (pgx), read slot info, return as object, close conn.

	conn, err := pgconn.Connect(context.Background(), os.Getenv("WALG_CONN_STRING")+" replication=yes")
	tracelog.ErrorLogger.FatalOnError(err)
	defer conn.Close(context.Background())

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos.String(), "DBName:", sysident.DBName)
	XLogPos = sysident.XLogPos

  tracelog.InfoLogger.Println("Trying to create the replication slot")
	_, err = pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, "",
																           pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.PhysicalReplication})
	// tracelog.ErrorLogger.FatalOnError(err)
  tracelog.DebugLogger.Println("Replication slot created.")
	// XLogPos, err = pglogrepl.ParseLSN(res.ConsistentPoint)
	// tracelog.ErrorLogger.FatalOnError(err)
  // tracelog.DebugLogger.Printf("Consistent point: %s", XLogPos.String())

  tracelog.DebugLogger.Printf("Starting replication from %s: ", XLogPos.String())
	err = pglogrepl.StartReplication(context.Background(), conn, slotName, XLogPos, pglogrepl.StartReplicationOptions{Timeline: sysident.Timeline, Mode: pglogrepl.PhysicalReplication})
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
				tracelog.DebugLogger.Println("XLogData =>", "WALStart", xld.WALStart, "ServerWALEnd", xld.ServerWALEnd, "ServerTime:", xld.ServerTime, "WALData", string(xld.WALData))

				XLogPos = xld.WALStart + pglogrepl.LSN(len(xld.WALData))
			}
		default:
			tracelog.DebugLogger.Printf("Received unexpected message: %#v\n", msg)
		}
	}
}
