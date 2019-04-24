package internal

import (
	"fmt"
	"github.com/jedib0t/go-pretty/table"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/tracelog"
	"io"
	"os"
	"text/tabwriter"
	"time"
)

// TODO : unit tests
// HandleBackupList is invoked to perform wal-g backup-list
func HandleBackupList(folder storage.Folder, pretty bool, json bool) {
	backups, err := getBackups(folder)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}

	if json {
		WriteBackupListAsJson(backups, os.Stdout, pretty)
	} else if pretty {
		WritePrettyBackupList(backups, os.Stdout)
	} else {
		WriteBackupList(backups, os.Stdout)
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
func WriteBackupListAsJson(backups []BackupTime, output io.Writer, pretty bool) {
	fmt.Print("[")
	for i, b := range backups {
		if i != 0 {
			fmt.Print(", ")
		}
		if pretty {
			fmt.Println()
			fmt.Print("\t")
		}
		_, _ = fmt.Fprintf(
			output,
			"{\"name\":\"%s\", \"last_modified\":\"%s\", \"wal_segment_backup_start\":\"%s\"}",
			b.BackupName,
			b.Time.Format(time.RFC3339),
			b.WalFileName)
	}
	if pretty {
		fmt.Println()
	}
	fmt.Print("]")
}
