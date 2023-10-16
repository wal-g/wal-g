package mysql

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type PrevBackupInfo struct {
	name           string
	fullBackupName *string
	sentinel       StreamSentinelDto
}

type DeltaBackupConfigurator interface {
	Configure(isFullBackup bool, hostname string, serverUUID string, serverVersion string) (PrevBackupInfo, int, error)
}

type NoDeltaBackupConfigurator struct{}

func NewNoDeltaBackupConfigurator() NoDeltaBackupConfigurator {
	return NoDeltaBackupConfigurator{}
}

// incrementCount is increment level of new backup
func (c NoDeltaBackupConfigurator) Configure(_ bool, _ string, _ string, _ string) (prevBackupInfo PrevBackupInfo, incrementCount int, err error) {
	return PrevBackupInfo{}, 0, nil
}

type RegularDeltaBackupConfigurator struct {
	folder            storage.Folder
	deltaBaseSelector internal.BackupSelector
}

func NewRegularDeltaBackupConfigurator(folder storage.Folder, deltaBaseSelector internal.BackupSelector) RegularDeltaBackupConfigurator {
	return RegularDeltaBackupConfigurator{folder, deltaBaseSelector}
}

// incrementCount is increment level of new backup
//
//nolint:funlen,gocyclo
func (c RegularDeltaBackupConfigurator) Configure(
	isFullBackup bool,
	hostname string,
	serverUUID string,
	serverVersion string) (prevBackupInfo PrevBackupInfo, incrementCount int, err error) {
	if isFullBackup {
		tracelog.InfoLogger.Println("Full backup requested.")
		return prevBackupInfo, 0, nil
	}
	maxDeltas, fromFull := internal.GetDeltaConfig()
	if maxDeltas == 0 {
		tracelog.InfoLogger.Println("WALG_DELTA_MAX_STEPS reached. Doing full backup.")
		return PrevBackupInfo{}, 0, nil
	}

	baseBackupFolder := c.folder.GetSubFolder(utility.BaseBackupPath)
	previousBackup, err := c.deltaBaseSelector.Select(c.folder)
	if err != nil {
		if _, ok := err.(internal.NoBackupsFoundError); ok {
			tracelog.InfoLogger.Println("Couldn't find previous backup. Doing full backup.")
			return PrevBackupInfo{}, 0, nil
		}
		return PrevBackupInfo{}, 0, err
	}

	var prevBackupSentinelDto = StreamSentinelDto{}
	err = previousBackup.FetchSentinel(&prevBackupSentinelDto)
	tracelog.ErrorLogger.FatalOnError(err)

	if prevBackupSentinelDto.IncrementCount != nil {
		incrementCount = *prevBackupSentinelDto.IncrementCount + 1
	} else {
		incrementCount = 1 // FIXME why '1'?
	}

	if incrementCount > maxDeltas {
		tracelog.InfoLogger.Println("Reached max delta steps. Doing full backup.")
		return PrevBackupInfo{}, 0, nil
	}

	// When WALG_DELTA_ORIGIN = 'LATEST_FULL' we always make delta backup from full backup
	// (incremental backups are not allowed)
	var prevBackupName = previousBackup.Name
	var prevFullBackupName *string
	if prevBackupSentinelDto.IsIncremental {
		prevFullBackupName = prevBackupSentinelDto.IncrementFullName
	} else {
		prevFullBackupName = &previousBackup.Name
	}
	if fromFull {
		tracelog.InfoLogger.Println("Delta will be made from full backup.")

		prevName := previousBackup.Name
		if prevBackupSentinelDto.IncrementFullName != nil /* && previousBackup.Name != prevBackupSentinelDto.IncrementFullName */ {
			prevName = *prevBackupSentinelDto.IncrementFullName
		}

		previousBackup, err = internal.NewBackup(baseBackupFolder, prevName)
		if err != nil {
			return PrevBackupInfo{}, 0, err
		}
		err = previousBackup.FetchSentinel(&prevBackupSentinelDto)
		if err != nil {
			return PrevBackupInfo{}, 0, err
		}
	}

	if prevBackupSentinelDto.LSN == nil {
		tracelog.InfoLogger.Println("Previous backup was made without support for delta feature. " +
			"Fallback to full backup with LSN marker for future deltas.")
		return PrevBackupInfo{}, 0, nil
	}

	if prevBackupSentinelDto.Hostname == "" || prevBackupSentinelDto.Hostname != hostname {
		tracelog.InfoLogger.Printf("Previous backup was made from another host (%s vs %s). "+
			"Fallback to full backup.", prevBackupSentinelDto.Hostname, hostname)
		return PrevBackupInfo{}, 0, nil
	}

	if prevBackupSentinelDto.ServerUUID == "" || prevBackupSentinelDto.ServerUUID != serverUUID {
		tracelog.InfoLogger.Printf("Server UUID has changed since last backup (%s vs %s). "+
			"Fallback to full backup.", prevBackupSentinelDto.ServerUUID, serverUUID)
		return PrevBackupInfo{}, 0, nil
	}

	if prevBackupSentinelDto.ServerVersion == "" || prevBackupSentinelDto.ServerVersion != serverVersion {
		tracelog.InfoLogger.Printf("Server version has changed since last backup (%s vs %s). "+
			"Fallback to full backup.", prevBackupSentinelDto.ServerVersion, serverVersion)
		return PrevBackupInfo{}, 0, nil
	}

	tracelog.InfoLogger.Printf("Delta backup from %s with LSN %v.\n", previousBackup.Name, prevBackupSentinelDto.LSN)
	prevBackupInfo = PrevBackupInfo{
		prevBackupName,
		prevFullBackupName,
		prevBackupSentinelDto}

	return prevBackupInfo, incrementCount, nil
}
