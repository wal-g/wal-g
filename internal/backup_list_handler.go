package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"text/tabwriter"
	"time"

	"github.com/jedib0t/go-pretty/table"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
)

type InfoLogger interface {
	Println(v ...interface{})
}

type ErrorLogger interface {
	FatalOnError(err error)
}

type Logging struct {
	InfoLogger  InfoLogger
	ErrorLogger ErrorLogger
}

func DefaultHandleBackupList(folder storage.Folder) {
	getBackupsFunc := func() ([]BackupTime, error) {
		return GetBackups(folder)
	}
	writeBackupListFunc := func(backups []BackupTime) {
		WriteBackupList(backups, os.Stdout)
	}
	logging := Logging{
		InfoLogger:  tracelog.InfoLogger,
		ErrorLogger: tracelog.ErrorLogger,
	}

	HandleBackupList(getBackupsFunc, writeBackupListFunc, logging)
}

func HandleBackupList(
	getBackupsFunc func() ([]BackupTime, error),
	writeBackupListFunc func([]BackupTime),
	logging Logging,
) {
	backups, err := getBackupsFunc()
	if len(backups) == 0 {
		logging.InfoLogger.Println("No backups found")
		return
	}
	logging.ErrorLogger.FatalOnError(err)

	writeBackupListFunc(backups)
}

func WriteBackupList(backups []BackupTime, output io.Writer) {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer writer.Flush()
	fmt.Fprintln(writer, "name\tlast_modified\twal_segment_backup_start")
	for i := len(backups) - 1; i >= 0; i-- {
		b := backups[i]
		_, _ = fmt.Fprintf(writer, "%v\t%v\t%v\n", b.BackupName, b.Time.Format(time.RFC3339), b.WalFileName)
	}
}

func WritePrettyBackupList(backups []BackupTime, output io.Writer) {
	writer := table.NewWriter()
	writer.SetOutputMirror(output)
	defer writer.Render()
	writer.AppendHeader(table.Row{"#", "Name", "Last modified", "WAL segment backup start"})
	for i, b := range backups {
		writer.AppendRow(table.Row{i, b.BackupName, b.Time.Format(time.RFC850), b.WalFileName})
	}
}

func WriteAsJSON(data interface{}, output io.Writer, pretty bool) error {
	var bytes []byte
	var err error
	if pretty {
		bytes, err = json.MarshalIndent(data, "", "    ")
	} else {
		bytes, err = json.Marshal(data)
	}
	if err != nil {
		return err
	}
	_, err = output.Write(bytes)
	return err
}
