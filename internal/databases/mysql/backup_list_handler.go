package mysql

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/printlist"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

type BackupDetail struct {
	BackupName string    `json:"backup_name"`
	ModifyTime time.Time `json:"modify_time"`

	BinLogStart    string    `json:"binlog_start"`
	BinLogEnd      string    `json:"binlog_end"`
	StartLocalTime time.Time `json:"start_local_time"`
	StopLocalTime  time.Time `json:"stop_local_time"`

	// these fields were introduced in
	// https://github.com/wal-g/wal-g/pull/930
	// so some old sentinels may not contain them
	UncompressedSize int64  `json:"uncompressed_size,omitempty"`
	CompressedSize   int64  `json:"compressed_size,omitempty"`
	Hostname         string `json:"hostname,omitempty"`

	IsPermanent bool        `json:"is_permanent"`
	UserData    interface{} `json:"user_data,omitempty"`
}

func (bd *BackupDetail) PrintableFields() []printlist.TableField {
	prettyModifyTime := internal.PrettyFormatTime(bd.ModifyTime)
	prettyStartTime := internal.PrettyFormatTime(bd.StartLocalTime)
	prettyStopTime := internal.PrettyFormatTime(bd.StopLocalTime)
	return []printlist.TableField{
		{
			Name:       "name",
			PrettyName: "Name",
			Value:      bd.BackupName,
		},
		{
			Name:        "last_modified",
			PrettyName:  "Last modified",
			Value:       internal.FormatTime(bd.ModifyTime),
			PrettyValue: &prettyModifyTime,
		},
		{
			Name:        "start_time",
			PrettyName:  "Start time",
			Value:       internal.FormatTime(bd.StartLocalTime),
			PrettyValue: &prettyStartTime,
		},
		{
			Name:        "stop_time",
			PrettyName:  "Stop time",
			Value:       internal.FormatTime(bd.StopLocalTime),
			PrettyValue: &prettyStopTime,
		},
		{
			Name:       "hostname",
			PrettyName: "Hostname",
			Value:      bd.Hostname,
		},
		{
			Name:        "binlog_start",
			PrettyName:  "Binlog start",
			Value:       bd.BinLogStart,
			PrettyValue: nil,
		},
		{
			Name:        "binlog_end",
			PrettyName:  "Binlog end",
			Value:       bd.BinLogEnd,
			PrettyValue: nil,
		},
		{
			Name:       "uncompressed_size",
			PrettyName: "Uncompressed size",
			Value:      strconv.FormatInt(bd.UncompressedSize, 10),
		},
		{
			Name:       "compressed_size",
			PrettyName: "Compressed size",
			Value:      strconv.FormatInt(bd.CompressedSize, 10),
		},
		{
			Name:       "is_permanent",
			PrettyName: "Permanent",
			Value:      fmt.Sprintf("%v", bd.IsPermanent),
		},
	}
}

//nolint:gocritic
func NewBackupDetail(backupTime internal.BackupTime, sentinel StreamSentinelDto) BackupDetail {
	return BackupDetail{
		BackupName:       backupTime.BackupName,
		ModifyTime:       backupTime.Time,
		BinLogStart:      sentinel.BinLogStart,
		BinLogEnd:        sentinel.BinLogEnd,
		StartLocalTime:   sentinel.StartLocalTime,
		StopLocalTime:    sentinel.StopLocalTime,
		UncompressedSize: sentinel.UncompressedSize,
		CompressedSize:   sentinel.CompressedSize,
		Hostname:         sentinel.Hostname,
		IsPermanent:      sentinel.IsPermanent,
		UserData:         sentinel.UserData,
	}
}

// TODO: unit tests
func HandleDetailedBackupList(folder storage.Folder, pretty, json bool) {
	backupTimes, err := internal.GetBackups(folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch list of backups in storage: %s", err)

	backupDetails := make([]BackupDetail, 0, len(backupTimes))
	for _, backupTime := range backupTimes {
		backup, err := internal.NewBackup(folder, backupTime.BackupName)
		tracelog.ErrorLogger.FatalOnError(err)

		var sentinel StreamSentinelDto
		err = backup.FetchSentinel(&sentinel)
		tracelog.ErrorLogger.FatalfOnError("Failed to load sentinel for backup %s", err)

		backupDetails = append(backupDetails, NewBackupDetail(backupTime, sentinel))
	}

	printableEntities := make([]printlist.Entity, len(backupDetails))
	for i := range backupDetails {
		printableEntities[i] = &backupDetails[i]
	}
	err = printlist.List(printableEntities, os.Stdout, pretty, json)
	tracelog.ErrorLogger.FatalfOnError("Print backups: %v", err)
}
