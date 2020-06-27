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
	"time"
	"strconv"
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
	var slotActive bool

	conn, err := pgconn.Connect(context.Background(), os.Getenv("WALG_CONN_STRING"))
	tracelog.ErrorLogger.FatalOnError(err)
	defer conn.Close(context.Background())

	sysident, err := pglogrepl.IdentifySystem(context.Background(), conn)
	tracelog.ErrorLogger.FatalOnError(err)
	tracelog.DebugLogger.Println("SystemID:", sysident.SystemID, "Timeline:", sysident.Timeline, "XLogPos:", sysident.XLogPos.String(), "DBName:", sysident.DBName)
	XLogPos = sysident.XLogPos

	// Check WALG_SLOTNAME env variable
	slotName := os.Getenv("WALG_SLOTNAME")
	if slotName == "" {
		tracelog.DebugLogger.Println("Using replication slots is disabled.")
		// Using XLogPos = sysident.XLogPos
	} else {
		tracelog.DebugLogger.Printf("Using slotname %s: ", slotName)
		// Fetch replication slot info
		result := conn.ExecParams(context.Background(), "select restart_lsn, active from pg_replication_slots where slot_name = $1 and slot_type = 'physical'", [][]byte{[]byte(slotName)}, nil, nil, nil)
		if result.NextRow() {
			slotActive, err = strconv.ParseBool(string(result.Values()[1]))
			tracelog.ErrorLogger.FatalOnError(err)
			if slotActive {
				tracelog.ErrorLogger.FatalOnError(newGenericWalReceiveError("slot is already active"))
			}
			XLogPos, err = pglogrepl.ParseLSN(string(result.Values()[0]))
			tracelog.ErrorLogger.FatalOnError(err)
		} else {
			// Create replication slot
			res, err := pglogrepl.CreateReplicationSlot(context.Background(), conn, slotName, "",
																		           pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.PhysicalReplication})
			tracelog.ErrorLogger.FatalOnError(err)
			XLogPos, err = pglogrepl.ParseLSN(res.ConsistentPoint)
			tracelog.ErrorLogger.FatalOnError(err)
		}
	}

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







	// err = conn.StartReplication (slotName string, startLsn uint64, timeline int64, pluginArguments ...string)
	// tracelog.ErrorLogger.FatalOnError(err)
	// while  {
	// 	msg, err := conn.WaitForReplicationMessage(timeout time.Duration) (r *ReplicationMessage, err error)
	// 	if err != nil && err != pgx.ErrNotificationTimeout {
	// 		tracelog.ErrorLogger.Printf("Error %+v while waiting for Replication Message. Will keep trying.", err)
	// 	}
	//
	// 	// ******************
	// 	// Handle the message
	// 	// ******************
	// 	tracelog.DebugLogger.Printf("Received wal segment %+v.", msg)
	// 	// ******************
	//
	// 	status, err := pgx.NewStandbyStatus(curlsn unint64)
	// 	if err != nil {
	// 		tracelog.ErrorLogger.Printf("Error %+v while creating StandbyStatus.", err)
	// 	} else {
	// 		err = SendStandbyStatus(status)
	// 		if err != nil && err != pgx.ErrNotificationTimeout {
	// 			tracelog.ErrorLogger.Printf("Error %+v while updating StandbyStatus.", err)
	// 		}
	// 	}
	// }
	//
	//
	//
	//
	//
	//
	// if uploader.ArchiveStatusManager.isWalAlreadyUploaded(tmpFilePath) {
	// 	err := uploader.ArchiveStatusManager.unmarkWalFile(tmpFilePath)
	//
	// 	if err != nil {
	// 		tracelog.ErrorLogger.Printf("unmark wal-g status for %s file failed due following error %+v", walFilePath, err)
	// 	}
	// 	return
	// }
	//
	// uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.WalPath)
	//
	// concurrency, err := getMaxUploadConcurrency()
	// tracelog.ErrorLogger.FatalOnError(err)
	//
	// preventWalOverwrite := viper.GetBool(PreventWalOverwriteSetting)
	//
	// bgUploader := NewBgUploader(walFilePath, int32(concurrency-1), uploader, preventWalOverwrite)
	// // Look for new WALs while doing main upload
	// bgUploader.Start()
	// err = uploadWALFile(uploader, walFilePath, bgUploader.preventWalOverwrite)
	// tracelog.ErrorLogger.FatalOnError(err)
	//
	// bgUploader.Stop()
	// if uploader.getUseWalDelta() {
	// 	uploader.deltaFileManager.FlushFiles(uploader.clone())
	// }
// } //
//
// // TODO : unit tests
// // uploadWALFile from FS to the cloud
// func uploadWALFile(uploader *Uploader, walFilePath string, preventWalOverwrite bool) error {
// 	if preventWalOverwrite {
// 		overwriteAttempt, err := checkWALOverwrite(uploader, walFilePath)
// 		if overwriteAttempt {
// 			return err
// 		} else if err != nil {
// 			return errors.Wrap(err, "Couldn't check whether there is an overwrite attempt due to inner error")
// 		}
// 	}
// 	walFile, err := os.Open(walFilePath)
// 	if err != nil {
// 		return errors.Wrapf(err, "upload: could not open '%s'\n", walFilePath)
// 	}
// 	err = uploader.UploadWalFile(walFile)
// 	return errors.Wrapf(err, "upload: could not Upload '%s'\n", walFilePath)
// }
//
// // TODO : unit tests
// func checkWALOverwrite(uploader *Uploader, walFilePath string) (overwriteAttempt bool, err error) {
// 	walFileReader, err := DownloadAndDecompressWALFile(uploader.UploadingFolder, filepath.Base(walFilePath))
// 	if err != nil {
// 		if _, ok := err.(ArchiveNonExistenceError); ok {
// 			err = nil
// 		}
// 		return false, err
// 	}
//
// 	archived, err := ioutil.ReadAll(walFileReader)
// 	if err != nil {
// 		return false, err
// 	}
//
// 	localBytes, err := ioutil.ReadFile(walFilePath)
// 	if err != nil {
// 		return false, err
// 	}
//
// 	if !bytes.Equal(archived, localBytes) {
// 		return true, newCantOverwriteWalFileError(walFilePath)
// 	} else {
// 		tracelog.InfoLogger.Printf("WAL file '%s' already archived with equal content, skipping", walFilePath)
// 		return true, nil
// 	}
// }
