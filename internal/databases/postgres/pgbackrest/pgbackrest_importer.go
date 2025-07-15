package pgbackrest

import (
	"strconv"
	"time"

	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/printlist"

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

func (bd *BackupDetails) PrintableFields() []printlist.TableField {
	prettyModifiedTime := internal.PrettyFormatTime(bd.ModifiedTime)
	prettyStartTime := internal.PrettyFormatTime(bd.StartTime)
	prettyFinishTime := internal.PrettyFormatTime(bd.FinishTime)
	return []printlist.TableField{
		{
			Name:       "name",
			PrettyName: "Name",
			Value:      bd.BackupName,
		},
		{
			Name:        "modified",
			PrettyName:  "Modified",
			Value:       internal.FormatTime(bd.ModifiedTime),
			PrettyValue: &prettyModifiedTime,
		},
		{
			Name:       "wal_file_name",
			PrettyName: "WAL file name",
			Value:      bd.WalFileName,
		},
		{
			Name:        "type",
			PrettyName:  "Type",
			Value:       bd.Type,
			PrettyValue: nil,
		},
		{
			Name:        "start_time",
			PrettyName:  "Start time",
			Value:       internal.FormatTime(bd.StartTime),
			PrettyValue: &prettyStartTime,
		},
		{
			Name:        "finish_time",
			PrettyName:  "Finish time",
			Value:       internal.FormatTime(bd.FinishTime),
			PrettyValue: &prettyFinishTime,
		},
		{
			Name:       "pg_version",
			PrettyName: "PG version",
			Value:      bd.PgVersion,
		},
		{
			Name:       "start_lsn",
			PrettyName: "Start LSN",
			Value:      bd.StartLsn.String(),
		},
		{
			Name:       "finish_lsn",
			PrettyName: "Finish LSN",
			Value:      bd.FinishLsn.String(),
		},
	}
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
