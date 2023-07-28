package greenplum

import (
	"fmt"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func NewSegDeltaBackupConfigurator(deltaBaseSelector internal.BackupSelector) SegDeltaBackupConfigurator {
	return SegDeltaBackupConfigurator{deltaBaseSelector}
}

type SegDeltaBackupConfigurator struct {
	deltaBaseSelector internal.BackupSelector
}

func (c SegDeltaBackupConfigurator) Configure(folder storage.Folder, isPermanent bool,
) (prevBackupInfo postgres.PrevBackupInfo, incrementCount int, err error) {
	baseBackupFolder := folder.GetSubFolder(utility.BaseBackupPath)
	previousBackup, err := c.deltaBaseSelector.Select(folder)
	if err != nil {
		return postgres.PrevBackupInfo{}, 0,
			fmt.Errorf("couldn't find the requested base backup: %w", err)
	}

	previousSegBackup, err := NewSegBackup(baseBackupFolder, previousBackup.Name)
	tracelog.ErrorLogger.FatalOnError(err)
	prevBackupSentinelDto, err := previousSegBackup.GetSentinel()
	tracelog.ErrorLogger.FatalOnError(err)

	if prevBackupSentinelDto.IncrementCount != nil {
		incrementCount = *prevBackupSentinelDto.IncrementCount + 1
	} else {
		incrementCount = 1
	}

	previousBackupMeta, err := previousSegBackup.FetchMeta()
	if err != nil {
		return postgres.PrevBackupInfo{}, 0,
			fmt.Errorf("failed to get previous backup metadata: %w", err)
	}

	if !isPermanent && previousBackupMeta.IsPermanent {
		return postgres.PrevBackupInfo{}, 0,
			fmt.Errorf("can't do a delta backup from permanent backup")
	}

	tracelog.InfoLogger.Printf("Delta backup from %v with LSN %s.\n", previousSegBackup.Name,
		*prevBackupSentinelDto.BackupStartLSN)

	sentinelDto, filesMetadataDto, err := previousSegBackup.GetSentinelAndFilesMetadata()
	if err != nil {
		return postgres.PrevBackupInfo{}, 0, err
	}

	prevBackupInfo = postgres.NewPrevBackupInfo(previousSegBackup.Name, sentinelDto, filesMetadataDto)
	return prevBackupInfo, incrementCount, err
}
