package postgres

import (
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type DeltaBackupConfigurator interface {
	Configure(folder storage.Folder, isPermanent bool) (PrevBackupInfo, int, error)
}

type RegularDeltaBackupConfigurator struct {
	deltaBaseSelector internal.BackupSelector
}

func NewRegularDeltaBackupConfigurator(deltaBaseSelector internal.BackupSelector) RegularDeltaBackupConfigurator {
	return RegularDeltaBackupConfigurator{deltaBaseSelector}
}

func (c RegularDeltaBackupConfigurator) Configure(
	folder storage.Folder, isPermanent bool,
) (prevBackupInfo PrevBackupInfo, incrementCount int, err error) {
	maxDeltas, fromFull := internal.GetDeltaConfig()
	if maxDeltas == 0 {
		return PrevBackupInfo{}, 0, nil
	}

	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	previousBackupName, err := c.deltaBaseSelector.Select(folder)
	if err != nil {
		if _, ok := err.(internal.NoBackupsFoundError); ok {
			tracelog.InfoLogger.Println("Couldn't find previous backup. Doing full backup.")
			return PrevBackupInfo{}, 0, nil
		}
		return PrevBackupInfo{}, 0, err
	}

	previousBackup := NewBackup(baseBackupFolder, previousBackupName)
	prevBackupSentinelDto, err := previousBackup.GetSentinel()
	tracelog.ErrorLogger.FatalOnError(err)

	if prevBackupSentinelDto.IncrementCount != nil {
		incrementCount = *prevBackupSentinelDto.IncrementCount + 1
	} else {
		incrementCount = 1
	}

	if incrementCount > maxDeltas {
		tracelog.InfoLogger.Println("Reached max delta steps. Doing full backup.")
		return PrevBackupInfo{}, 0, nil
	}

	if prevBackupSentinelDto.BackupStartLSN == nil {
		tracelog.InfoLogger.Println("LATEST backup was made without support for delta feature. " +
			"Fallback to full backup with LSN marker for future deltas.")
		return PrevBackupInfo{}, 0, nil
	}

	previousBackupMeta, err := previousBackup.FetchMeta()
	if err != nil {
		tracelog.InfoLogger.Printf(
			"Failed to get previous backup metadata: %s. Doing full backup.\n", err.Error())
		return PrevBackupInfo{}, 0, nil
	}

	if !isPermanent && !fromFull && previousBackupMeta.IsPermanent {
		tracelog.InfoLogger.Println("Can't do a delta backup from permanent backup. Doing full backup.")
		return PrevBackupInfo{}, 0, nil
	}

	if fromFull {
		tracelog.InfoLogger.Println("Delta will be made from full backup.")

		if prevBackupSentinelDto.IncrementFullName != nil {
			previousBackupName = *prevBackupSentinelDto.IncrementFullName
		}

		previousBackup := NewBackup(baseBackupFolder, previousBackupName)
		prevBackupSentinelDto, err = previousBackup.GetSentinel()
		if err != nil {
			return PrevBackupInfo{}, 0, err
		}
	}
	tracelog.InfoLogger.Printf("Delta backup from %v with LSN %s.\n", previousBackupName,
		*prevBackupSentinelDto.BackupStartLSN)
	prevBackupInfo.name = previousBackupName
	prevBackupInfo.sentinelDto, prevBackupInfo.filesMetadataDto, err = previousBackup.GetSentinelAndFilesMetadata()
	return prevBackupInfo, incrementCount, err
}

type CatchupDeltaBackupConfigurator struct {
	fakePrevSentinel BackupSentinelDto
}

func NewCatchupDeltaBackupConfigurator(fakePreviousBackupSentinelDto BackupSentinelDto) CatchupDeltaBackupConfigurator {
	return CatchupDeltaBackupConfigurator{
		fakePrevSentinel: fakePreviousBackupSentinelDto,
	}
}

func (c CatchupDeltaBackupConfigurator) Configure(storage.Folder, bool) (prevBackupInfo PrevBackupInfo, incrementCount int, err error) {
	prevBackupInfo.sentinelDto = c.fakePrevSentinel
	prevBackupInfo.filesMetadataDto = FilesMetadataDto{}
	return prevBackupInfo, 1, nil
}
