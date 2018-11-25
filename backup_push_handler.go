package walg

import (
	"github.com/wal-g/wal-g/tracelog"
	"os"
	"path/filepath"
	"strconv"
	"time"
)

// TODO : unit tests
func getDeltaConfig() (maxDeltas int, fromFull bool) {
	stepsStr, hasSteps := os.LookupEnv("WALG_DELTA_MAX_STEPS")
	var err error
	if hasSteps {
		maxDeltas, err = strconv.Atoi(stepsStr)
		if err != nil {
			tracelog.ErrorLogger.Fatal("Unable to parse WALG_DELTA_MAX_STEPS ", err)
		}
	}
	origin, hasOrigin := os.LookupEnv("WALG_DELTA_ORIGIN")
	if hasOrigin {
		switch origin {
		case "LATEST":
		case "LATEST_FULL":
			fromFull = true
		default:
			tracelog.ErrorLogger.Fatal("Unknown WALG_DELTA_ORIGIN:", origin)
		}
	}
	return
}

// TODO : unit tests
// HandleBackupPush is invoked to perform a wal-g backup-push
func HandleBackupPush(archiveDirectory string, uploader *Uploader) {
	archiveDirectory = ResolveSymlink(archiveDirectory)
	maxDeltas, fromFull := getDeltaConfig()

	var previousBackupSentinelDto BackupSentinelDto
	var previousBackupName string
	var err error
	incrementCount := 1

	if maxDeltas > 0 {
		previousBackupName, err = getLatestBackupKey(uploader.uploadingFolder)
		if err != nil {
			if _, ok := err.(NoBackupsFoundError); ok {
				tracelog.InfoLogger.Println("Couldn't find previous backup. Doing full backup.")
			} else {
				tracelog.ErrorLogger.FatalError(err)
			}
		} else {
			previousBackup := NewBackup(uploader.uploadingFolder, previousBackupName)
			previousBackupSentinelDto, err = previousBackup.fetchSentinel()
			if err != nil {
				tracelog.ErrorLogger.FatalError(err)
			}
			if previousBackupSentinelDto.IncrementCount != nil {
				incrementCount = *previousBackupSentinelDto.IncrementCount + 1
			}

			if incrementCount > maxDeltas {
				tracelog.InfoLogger.Println("Reached max delta steps. Doing full backup.")
				previousBackupSentinelDto = BackupSentinelDto{}
			} else if previousBackupSentinelDto.BackupStartLSN == nil {
				tracelog.InfoLogger.Println("LATEST backup was made without support for delta feature. Fallback to full backup with LSN marker for future deltas.")
			} else {
				if fromFull {
					tracelog.InfoLogger.Println("Delta will be made from full backup.")
					previousBackupName = *previousBackupSentinelDto.IncrementFullName
					previousBackup := NewBackup(uploader.uploadingFolder, previousBackupName)
					previousBackupSentinelDto, err = previousBackup.fetchSentinel()
					if err != nil {
						tracelog.ErrorLogger.FatalError(err)
					}
				}
				tracelog.InfoLogger.Printf("Delta backup from %v with LSN %x. \n", previousBackupName, *previousBackupSentinelDto.BackupStartLSN)
			}
		}
	} else {
		tracelog.InfoLogger.Println("Doing full backup.")
	}

	bundle := NewBundle(archiveDirectory, previousBackupSentinelDto.BackupStartLSN, previousBackupSentinelDto.Files)

	// Connect to postgres and start/finish a nonexclusive backup.
	conn, err := Connect()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	backupName, backupStartLSN, pgVersion, err := bundle.StartBackup(conn, time.Now().String())
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}

	if len(previousBackupName) > 0 && previousBackupSentinelDto.BackupStartLSN != nil {
		if uploader.useWalDelta {
			err = bundle.DownloadDeltaMap(uploader.uploadingFolder.GetSubFolder(WalPath), backupStartLSN)
			if err == nil {
				tracelog.InfoLogger.Println("Successfully loaded delta map, delta backup will be made with provided delta map")
			} else {
				tracelog.WarningLogger.Printf("Error during loading delta map: '%v'. Fallback to full scan delta backup\n", err)
			}
		}
		backupName = backupName + "_D_" + stripWalFileName(previousBackupName)
	}

	bundle.TarBallMaker = NewStorageTarBallMaker(backupName, uploader)

	// Start a new tar bundle, walk the archiveDirectory and upload everything there.
	bundle.StartQueue()
	tracelog.InfoLogger.Println("Walking ...")
	err = filepath.Walk(archiveDirectory, bundle.HandleWalkedFSObject)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	err = bundle.FinishQueue()
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	err = bundle.UploadPgControl(uploader.compressor.FileExtension())
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	// Stops backup and write/upload postgres `backup_label` and `tablespace_map` Files
	finishLsn, err := bundle.UploadLabelFiles(conn)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}

	timelineChanged := bundle.checkTimelineChanged(conn)
	var currentBackupSentinelDto *BackupSentinelDto

	if !timelineChanged {
		currentBackupSentinelDto = &BackupSentinelDto{
			BackupStartLSN:   &backupStartLSN,
			IncrementFromLSN: previousBackupSentinelDto.BackupStartLSN,
			PgVersion:        pgVersion,
		}
		if previousBackupSentinelDto.BackupStartLSN != nil {
			currentBackupSentinelDto.IncrementFrom = &previousBackupName
			if previousBackupSentinelDto.isIncremental() {
				currentBackupSentinelDto.IncrementFullName = previousBackupSentinelDto.IncrementFullName
			} else {
				currentBackupSentinelDto.IncrementFullName = &previousBackupName
			}
			currentBackupSentinelDto.IncrementCount = &incrementCount
		}

		currentBackupSentinelDto.setFiles(bundle.GetFiles())
		currentBackupSentinelDto.BackupFinishLSN = &finishLsn
	}

	// Wait for all uploads to finish.
	err = bundle.TarBall.Finish(currentBackupSentinelDto)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
}
