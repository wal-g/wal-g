package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jedib0t/go-pretty/table"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
)

// TODO : unit tests
// HandleBackupList is invoked to perform wal-g backup-list
func HandleBackupList(folder storage.Folder) {
	backups, err := getBackups(folder)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}

	WriteBackupList(backups, os.Stdout)
}

// TODO : unit tests
func HandleBackupListWithFlags(folder storage.Folder, pretty bool, json bool, detail bool) {
	backups, err := getBackups(folder)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}
	// if details are requested we append content of metadata.json to each line
	backupDetails := make([]BackupDetail, len(backups))
	if detail {
		for i := len(backups) - 1; i >= 0; i-- {
			backup, err := GetBackupByName(backups[i].BackupName, folder)
			if err != nil {
				tracelog.ErrorLogger.FatalError(err)
			} else {
				metaData, err := backup.FetchMeta()
				if err != nil {
					tracelog.ErrorLogger.FatalError(err)
				}
				backupDetails[i].writeBackupTime(backups[i])
				backupDetails[i].writeExtendedMetadataDto(metaData)
			}
		}
		if json {
			WriteBackupListDetailsAsJson(backupDetails, os.Stdout, pretty)
		} else if pretty {
			WritePrettyBackupListDetails(backupDetails, os.Stdout)
		} else {
			WriteBackupListDetails(backupDetails, os.Stdout)
		}
	} else {
		if json {
			WriteBackupListAsJson(backups, os.Stdout, pretty)
		} else if pretty {
			WritePrettyBackupList(backups, os.Stdout)
		} else {
			WriteBackupList(backups, os.Stdout)
		}
	}
}

// TODO : unit tests
func WriteBackupList(backups []BackupTime, output io.Writer) {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer writer.Flush()
	fmt.Fprintln(writer, "name\tlast_modified\twal_segment_backup_start")
	for i := len(backups) - 1; i >= 0; i-- {
		b := backups[i]
		fmt.Fprintln(writer, fmt.Sprintf("%v\t%v\t%v", b.BackupName, b.Time.Format(time.RFC3339), b.WalFileName))
	}
}

// TODO : unit tests
func WriteBackupListDetails(backupDetails []BackupDetail, output io.Writer) {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer writer.Flush()
	fmt.Fprintln(writer, "name\tlast_modified\twal_segment_backup_start\tstart_time\tfinish_time\thostname\tdata_dir\tpg_version\tstart_lsn\tfinish_lsn")
	for i := len(backupDetails) - 1; i >= 0; i-- {
		b := backupDetails[i]
		fmt.Fprintln(writer, fmt.Sprintf("%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v", b.BackupName, b.Time.Format(time.RFC3339), b.WalFileName, b.StartTime.Format(time.RFC850), b.FinishTime.Format(time.RFC850), b.Hostname, b.DataDir, b.PgVersion, b.StartLsn, b.FinishLsn))
	}
}

// TODO : unit tests
func WritePrettyBackupList(backups []BackupTime, output io.Writer) {
	writer := table.NewWriter()
	writer.SetOutputMirror(output)
	defer writer.Render()
	writer.AppendHeader(table.Row{"#", "Name", "Last modified", "WAL segment backup start"})
	for i, b := range backups {
		writer.AppendRow(table.Row{i, b.BackupName, b.Time.Format(time.RFC850), b.WalFileName})
	}
}

// TODO : unit tests
func WritePrettyBackupListDetails(backupDetails []BackupDetail, output io.Writer) {
	writer := table.NewWriter()
	writer.SetOutputMirror(output)
	defer writer.Render()
	writer.AppendHeader(table.Row{"#", "Name", "Last modified", "WAL segment backup start", "Start time", "Finish time", "Hostname", "Datadir", "PG Version", "Start LSN", "Finish LSN"})
	for i, b := range backupDetails {
		writer.AppendRow(table.Row{i, b.BackupName, b.Time.Format(time.RFC850), b.WalFileName, b.StartTime.Format(time.RFC850), b.FinishTime.Format(time.RFC850), b.Hostname, b.DataDir, b.PgVersion, b.StartLsn, b.FinishLsn})
	}
}

// TODO : unit tests
func WriteBackupListAsJson(backups []BackupTime, output io.Writer, pretty bool) {
	var bytes []byte
	var _ error
	if pretty {
		bytes, _ = json.MarshalIndent(backups, "", "    ")
	} else {
		bytes, _ = json.Marshal(backups)
	}
	output.Write(bytes)
}

// TODO : unit tests
func WriteBackupListDetailsAsJson(backupDetails []BackupDetail, output io.Writer, pretty bool) {
	var bytes []byte
	var _ error
	if pretty {
		bytes, _ = json.MarshalIndent(backupDetails, "", "    ")
	} else {
		bytes, _ = json.Marshal(backupDetails)
	}
	output.Write(bytes)
}
