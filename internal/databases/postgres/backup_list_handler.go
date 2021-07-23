package postgres

import (
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/jedib0t/go-pretty/table"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// TODO : unit tests
func HandleDetailedBackupList(folder storage.Folder, pretty bool, json bool) {
	backups, err := internal.GetBackups(folder)

	if len(backups) == 0 {
		tracelog.InfoLogger.Println("No backups found")
		return
	}
	tracelog.ErrorLogger.FatalOnError(err)

	// if details are requested we append content of metadata.json to each line

	backupDetails, err := GetBackupsDetails(folder, backups)
	tracelog.ErrorLogger.FatalOnError(err)
	SortBackupDetails(backupDetails)

	switch {
	case json:
		err = internal.WriteAsJSON(backupDetails, os.Stdout, pretty)
	case pretty:
		WritePrettyBackupListDetails(backupDetails, os.Stdout)
	default:
		err = WriteBackupListDetails(backupDetails, os.Stdout)
	}
	tracelog.ErrorLogger.FatalOnError(err)
}

// TODO : unit tests
func WriteBackupListDetails(backupDetails []BackupDetail, output io.Writer) error {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer writer.Flush()
	//nolint:lll
	_, err := fmt.Fprintln(writer, "name\tmodified\twal_segment_backup_start\tstart_time\tfinish_time\thostname\tdata_dir\tpg_version\tstart_lsn\tfinish_lsn\tis_permanent")
	if err != nil {
		return err
	}
	for i := 0; i < len(backupDetails); i++ {
		b := backupDetails[i]
		//nolint:lll
		_, err = fmt.Fprintf(writer, "%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\n", b.BackupName, internal.FormatTime(b.Time), b.WalFileName, internal.FormatTime(b.StartTime), internal.FormatTime(b.FinishTime), b.Hostname, b.DataDir, b.PgVersion, b.StartLsn, b.FinishLsn, b.IsPermanent)
		if err != nil {
			return err
		}
	}
	return nil
}

// TODO : unit tests
func WritePrettyBackupListDetails(backupDetails []BackupDetail, output io.Writer) {
	writer := table.NewWriter()
	writer.SetOutputMirror(output)
	defer writer.Render()
	//nolint:lll
	writer.AppendHeader(table.Row{"#", "Name", "Modified", "WAL segment backup start", "Start time", "Finish time", "Hostname", "Datadir", "PG Version", "Start LSN", "Finish LSN", "Permanent"})
	for idx := range backupDetails {
		b := &backupDetails[idx]
		writer.AppendRow(
			table.Row{idx, b.BackupName, internal.PrettyFormatTime(b.Time), b.WalFileName,
				internal.PrettyFormatTime(b.StartTime), internal.PrettyFormatTime(b.FinishTime),
				b.Hostname, b.DataDir, b.PgVersion, b.StartLsn, b.FinishLsn, b.IsPermanent})
	}
}
