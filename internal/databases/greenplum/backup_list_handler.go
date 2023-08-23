package greenplum

import (
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

func NewBackupDetail(backup Backup) BackupDetail {
	return BackupDetail{
		Name:              backup.Name,
		RestorePoint:      backup.SentinelDto.RestorePoint,
		UserData:          backup.SentinelDto.UserData,
		StartTime:         backup.SentinelDto.StartTime,
		FinishTime:        backup.SentinelDto.FinishTime,
		DatetimeFormat:    backup.SentinelDto.DatetimeFormat,
		Hostname:          backup.SentinelDto.Hostname,
		GpVersion:         backup.SentinelDto.GpVersion,
		IsPermanent:       backup.SentinelDto.IsPermanent,
		SystemIdentifier:  backup.SentinelDto.SystemIdentifier,
		UncompressedSize:  backup.SentinelDto.UncompressedSize,
		CompressedSize:    backup.SentinelDto.CompressedSize,
		DataCatalogSize:   backup.SentinelDto.DataCatalogSize,
		IncrementFrom:     backup.SentinelDto.IncrementFrom,
		IncrementFullName: backup.SentinelDto.IncrementFullName,
		IncrementCount:    backup.SentinelDto.IncrementCount,
	}
}

type BackupDetail struct {
	Name         string
	RestorePoint *string     `json:"restore_point,omitempty"`
	UserData     interface{} `json:"user_data,omitempty"`

	StartTime        time.Time `json:"start_time"`
	FinishTime       time.Time `json:"finish_time"`
	DatetimeFormat   string    `json:"date_fmt,omitempty"`
	Hostname         string    `json:"hostname"`
	GpVersion        string    `json:"gp_version"`
	IsPermanent      bool      `json:"is_permanent"`
	SystemIdentifier *uint64   `json:"system_identifier,omitempty"`

	UncompressedSize int64 `json:"uncompressed_size"`
	CompressedSize   int64 `json:"compressed_size"`
	DataCatalogSize  int64 `json:"data_catalog_size"`

	IncrementFrom     *string `json:"increment_from,omitempty"`
	IncrementFullName *string `json:"increment_full_name,omitempty"`
	IncrementCount    *int    `json:"increment_count,omitempty"`
}

//TODO: Implement backup-list handler

// ListStorageBackups returns the list of storage backups sorted by finish time (in ascending order)
func ListStorageBackups(folder storage.Folder) ([]Backup, error) {
	backupObjects, err := internal.GetBackups(folder.GetSubFolder(utility.BaseBackupPath))
	if err != nil {
		return nil, fmt.Errorf("failed to fetch list of backups in storage: %w", err)
	}

	backups := make([]Backup, 0, len(backupObjects))
	for _, b := range backupObjects {
		backup, err := NewBackup(folder, b.BackupName)
		if err != nil {
			return nil, err
		}

		_, err = backup.GetSentinel()
		if err != nil {
			return nil, fmt.Errorf("failed to load sentinel for backup %s: %w", b.BackupName, err)
		}

		backups = append(backups, backup)
	}

	sort.Slice(backups, func(i, j int) bool {
		return backups[i].SentinelDto.FinishTime.Before(backups[j].SentinelDto.FinishTime)
	})

	return backups, nil
}

func MakeBackupDetails(backups []Backup) []BackupDetail {
	details := make([]BackupDetail, 0)
	for i := range backups {
		details = append(details, NewBackupDetail(backups[i]))
	}
	return details
}

func HandleDetailedBackupList(folder storage.Folder, pretty, json bool) {
	if !json {
		tracelog.ErrorLogger.Fatalf("non-json detailed backup list is not supported (yet)")
	}

	backups, err := ListStorageBackups(folder)

	if len(backups) == 0 {
		tracelog.InfoLogger.Println("No backups found")
		return
	}
	tracelog.ErrorLogger.FatalOnError(err)

	err = internal.WriteAsJSON(MakeBackupDetails(backups), os.Stdout, pretty)
	tracelog.ErrorLogger.FatalOnError(err)
}
