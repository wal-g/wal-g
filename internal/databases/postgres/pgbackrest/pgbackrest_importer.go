package pgbackrest

import (
	"strconv"
	"time"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type BackupDetails struct {
	BackupName           string
	ModifiedTime         time.Time
	WalFileName          string
	Type                 string
	StartTime            time.Time
	FinishTime           time.Time
	PgVersion            string
	StartLsn             postgres.LSN
	FinishLsn            postgres.LSN
	SystemIdentifier     uint64
	DirectoryPaths       []string
	DefaultFileMode      int
	DefaultDirectoryMode int
}

func GetBackupList(backupsFolder storage.Folder, stanza string) ([]internal.BackupTime, error) {
	backupsSettings, err := LoadBackupsSettings(backupsFolder, stanza)
	if err != nil {
		return nil, err
	}

	var backupTimes []internal.BackupTime
	for i := range backupsSettings {
		backupTimes = append(backupTimes, internal.BackupTime{
			BackupName:  backupsSettings[i].Name,
			Time:        getTime(backupsSettings[i].BackupTimestampStop),
			WalFileName: backupsSettings[i].BackupArchiveStart,
		})
	}
	return backupTimes, nil
}

func GetBackupListWithMetadata(backupsFolder storage.Folder, metaFetcher internal.GenericMetaFetcher,
	stanza string) ([]internal.BackupTimeWithMetadata, error) {
	backupsSettings, err := LoadBackupsSettings(backupsFolder, stanza)
	if err != nil {
		return nil, err
	}

	var backupTimes []internal.BackupTimeWithMetadata
	for i := range backupsSettings {
		metadata, err := metaFetcher.Fetch(backupsSettings[i].Name, backupsFolder)
		if err != nil {
			return nil, err
		}
		backupTimes = append(backupTimes, internal.BackupTimeWithMetadata{
			BackupTime: internal.BackupTime{
				BackupName:  backupsSettings[i].Name,
				Time:        getTime(backupsSettings[i].BackupTimestampStop),
				WalFileName: backupsSettings[i].BackupArchiveStart,
			},
			GenericMetadata: metadata,
		})
	}
	return backupTimes, nil
}

func GetBackupDetails(backupsFolder storage.Folder, stanza string, backupName string) (*BackupDetails, error) {
	manifest, err := LoadManifest(backupsFolder, stanza, backupName)
	if err != nil {
		return nil, err
	}

	backupTime := internal.BackupTime{
		BackupName:  manifest.BackupSection.BackupLabel,
		Time:        getTime(manifest.BackupSection.BackupTimestampStop),
		WalFileName: manifest.BackupSection.BackupArchiveStart,
	}

	startLsn, err := postgres.ParseLSN(manifest.BackupSection.BackupLsnStart)
	if err != nil {
		return nil, err
	}

	finishLsn, err := postgres.ParseLSN(manifest.BackupSection.BackupLsnStop)
	if err != nil {
		return nil, err
	}

	fileMode, err := strconv.ParseInt(manifest.DefaultFileSection.Mode, 8, 0)
	if err != nil {
		return nil, err
	}
	directoryMode, err := strconv.ParseInt(manifest.DefaultPathSection.Mode, 8, 0)
	if err != nil {
		return nil, err
	}

	backupDetails := BackupDetails{
		BackupName:           backupTime.BackupName,
		ModifiedTime:         backupTime.Time,
		WalFileName:          backupTime.WalFileName,
		Type:                 manifest.BackupSection.BackupType,
		StartTime:            getTime(manifest.BackupSection.BackupTimestampStart),
		FinishTime:           getTime(manifest.BackupSection.BackupTimestampStop),
		PgVersion:            manifest.BackupDatabaseSection.Version,
		StartLsn:             startLsn,
		FinishLsn:            finishLsn,
		SystemIdentifier:     manifest.BackupDatabaseSection.SystemID,
		DirectoryPaths:       manifest.PathSection.directoryPaths,
		DefaultFileMode:      int(fileMode),
		DefaultDirectoryMode: int(directoryMode),
	}

	return &backupDetails, nil
}

func getTime(timestamp int64) time.Time {
	return time.Unix(timestamp, 0)
}
