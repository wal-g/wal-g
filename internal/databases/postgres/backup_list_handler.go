package postgres

import (
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
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

	backupDetails, err := GetBackupsDetails(folder, backups)
	tracelog.ErrorLogger.FatalOnError(err)
	SortBackupDetails(backupDetails)

	switch {
	case json:
		err = internal.WriteAsJSON(backupDetails, os.Stdout, pretty)
	case pretty:
		WritePrettyBackupList(backupDetails, os.Stdout)
	default:
		err = WriteBackupList(backupDetails, os.Stdout)
	}
	tracelog.ErrorLogger.FatalOnError(err)
}

var columns = []string{
	"name",
	"modified",
	"wal_segment_backup_start",
	"start_time",
	"finish_time",
	"hostname",
	"data_dir",
	"pg_version",
	"start_lsn",
	"finish_lsn",
	"is_permanent",
}

func WriteBackupList(backupDetails []BackupDetail, output io.Writer) error {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer func() { _ = writer.Flush() }()
	header := strings.Join(columns, "\t")
	_, err := fmt.Fprintln(writer, header)
	if err != nil {
		return err
	}
	for i := 0; i < len(backupDetails); i++ {
		b := backupDetails[i]
		fields := []string{
			b.BackupName,
			internal.FormatTime(b.Time),
			b.WalFileName,
			internal.FormatTime(b.StartTime),
			internal.FormatTime(b.FinishTime),
			b.Hostname,
			b.DataDir,
			strconv.Itoa(b.PgVersion),
			strconv.FormatUint(uint64(b.StartLsn), 10),
			strconv.FormatUint(uint64(b.FinishLsn), 10),
			fmt.Sprintf("%v", b.IsPermanent),
		}
		_, err = fmt.Fprintf(writer, strings.Join(fields, "\t"))
		if err != nil {
			return err
		}
	}
	return nil
}

var prettyColumns = []any{
	"#",
	"Name",
	"Modified",
	"WAL segment backup start",
	"Start time",
	"Finish time",
	"Hostname",
	"Datadir",
	"PG Version",
	"Start LSN",
	"Finish LSN",
	"Permanent",
}

func WritePrettyBackupList(backupDetails []BackupDetail, output io.Writer) {
	writer := table.NewWriter()
	writer.SetOutputMirror(output)
	defer writer.Render()
	writer.AppendHeader(prettyColumns)
	for idx := range backupDetails {
		b := &backupDetails[idx]
		row := table.Row{
			idx,
			b.BackupName,
			internal.PrettyFormatTime(b.Time),
			b.WalFileName,
			internal.PrettyFormatTime(b.StartTime),
			internal.PrettyFormatTime(b.FinishTime),
			b.Hostname,
			b.DataDir,
			b.PgVersion,
			b.StartLsn,
			b.FinishLsn,
			b.IsPermanent,
		}
		writer.AppendRow(row)
	}
}
