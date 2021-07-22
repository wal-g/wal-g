package mysql

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jedib0t/go-pretty/table"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/storages/storage"
)

type BackupDetail struct {
	BackupName string    `json:"backup_name"`
	ModifyTime time.Time `json:"modify_time"`

	BinLogStart    string    `json:"binlog_start"`
	BinLogEnd      string    `json:"binlog_end"`
	StartLocalTime time.Time `json:"start_local_time"`
	StopLocalTime  time.Time `json:"stop_local_time"`

	// these fields were introduced recently in
	// https://github.com/wal-g/wal-g/pull/930
	// so it is not guaranteed that sentinel contains them
	UncompressedSize int64  `json:"uncompressed_size,omitempty"`
	CompressedSize   int64  `json:"compressed_size,omitempty"`
	Hostname         string `json:"hostname,omitempty"`

	IsPermanent bool        `json:"is_permanent"`
	UserData    interface{} `json:"user_data,omitempty"`
}

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

func HandleDetailedBackupList(folder storage.Folder, pretty, json bool) {
	backupTimes, err := internal.GetBackups(folder)
	tracelog.ErrorLogger.FatalfOnError("Failed to fetch list of backups in storage: %s", err)

	backupDetails := make([]BackupDetail, 0, len(backupTimes))
	for _, backupTime := range backupTimes {
		backup := internal.NewBackup(folder, backupTime.BackupName)

		var sentinel StreamSentinelDto
		err = backup.FetchSentinel(&sentinel)
		tracelog.ErrorLogger.FatalfOnError("Failed to load sentinel for backup %s", err)

		backupDetails = append(backupDetails, NewBackupDetail(backupTime, sentinel))
	}

	switch {
	case json:
		err = internal.WriteAsJSON(backupDetails, os.Stdout, pretty)
	case pretty:
		writePrettyBackupListDetails(backupDetails, os.Stdout)
	default:
		err = writeBackupListDetails(backupDetails, os.Stdout)
	}
	tracelog.ErrorLogger.FatalOnError(err)
}

// TODO : unit tests
func writeBackupListDetails(backupDetails []BackupDetail, output io.Writer) error {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer writer.Flush()
	_, err := fmt.Fprintln(writer, "name\tlast_modified\tstart_time\tfinish_time\thostname\tbinlog_start\tbinlog_end\tuncompressed_size\tcompressed_size\tis_permanent") //nolint:lll
	if err != nil {
		return err
	}
	for i := len(backupDetails) - 1; i >= 0; i-- {
		b := backupDetails[i]
		_, err = fmt.Fprintf(writer, "%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v",
			b.BackupName, b.ModifyTime.Format(time.RFC3339), b.StartLocalTime.Format(time.RFC850), b.StopLocalTime.Format(time.RFC850), b.Hostname, b.BinLogStart, b.BinLogEnd, b.UncompressedSize, b.CompressedSize, b.IsPermanent) //nolint:lll
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO : unit tests
func writePrettyBackupListDetails(backupDetails []BackupDetail, output io.Writer) {
	writer := table.NewWriter()
	writer.SetOutputMirror(output)
	defer writer.Render()
	writer.AppendHeader(table.Row{"#", "Name", "Last modified", "Start time", "Finish time", "Hostname", "Binlog start", "Binlog end", "Uncompressed size", "Compressed size", "Permanent"}) //nolint:lll
	for idx := range backupDetails {
		b := &backupDetails[idx]
		writer.AppendRow(table.Row{idx, b.BackupName, b.ModifyTime.Format(time.RFC850), b.StartLocalTime.Format(time.RFC850), b.StopLocalTime.Format(time.RFC850), b.Hostname, b.BinLogStart, b.BinLogEnd, b.UncompressedSize, b.CompressedSize, b.IsPermanent}) //nolint:lll
	}
}
