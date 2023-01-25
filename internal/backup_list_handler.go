package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"text/tabwriter"

	"github.com/jedib0t/go-pretty/table"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/pkg/storages/storage"
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

// BackupTimeWithMetadata is used to sort backups by
// latest modified time or creation time.
type BackupTimeWithMetadata struct {
	BackupTime
	GenericMetadata
}

func DefaultHandleBackupList(folder storage.Folder, metaFetcher GenericMetaFetcher, pretty, json bool) {
	getBackupsFunc := func() ([]BackupTimeWithMetadata, error) {
		backupsWithMeta, err := GetBackupsWithMetadata(folder, metaFetcher)
		if _, ok := err.(NoBackupsFoundError); ok {
			err = nil
		}

		SortBackupTimeWithMetadataSlices(backupsWithMeta)

		return backupsWithMeta, err
	}

	writeBackupListFunc := func(backupsWithMetadata []BackupTimeWithMetadata) {
		switch {
		case json:
			backups := make([]BackupTime, len(backupsWithMetadata))
			for i := 0; i < len(backupsWithMetadata); i++ {
				backups[i] = backupsWithMetadata[i].BackupTime
			}
			err := WriteAsJSON(backupsWithMetadata, os.Stdout, pretty)
			tracelog.ErrorLogger.FatalOnError(err)
		case pretty:
			WritePrettyBackupList(backupsWithMetadata, os.Stdout)
		default:
			WriteBackupList(backupsWithMetadata, os.Stdout)
		}
	}
	logging := Logging{
		InfoLogger:  tracelog.InfoLogger,
		ErrorLogger: tracelog.ErrorLogger,
	}

	HandleBackupList(getBackupsFunc, writeBackupListFunc, logging)
}

func HandleBackupList(
	getBackupsFunc func() ([]BackupTimeWithMetadata, error),
	writeBackupListFunc func([]BackupTimeWithMetadata),
	logging Logging,
) {
	backups, err := getBackupsFunc()
	logging.ErrorLogger.FatalOnError(err)

	if len(backups) == 0 {
		logging.InfoLogger.Println("No backups found")
		return
	}

	writeBackupListFunc(backups)
}

func WriteBackupList(backups []BackupTimeWithMetadata, output io.Writer) {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer writer.Flush()
	fmt.Fprintln(writer, "name\tcreated\twal_segment_backup_start")
	for _, b := range backups {
		fmt.Fprintf(writer, "%v\t%v\t%v\n", b.BackupTime.BackupName, FormatTime(b.StartTime), b.WalFileName)
	}
}

func WritePrettyBackupList(backups []BackupTimeWithMetadata, output io.Writer) {
	writer := table.NewWriter()
	writer.SetOutputMirror(output)
	defer writer.Render()
	writer.AppendHeader(table.Row{"#", "Name", "Created", "WAL segment backup start"})
	for i, b := range backups {
		writer.AppendRow(table.Row{i, b.BackupTime.BackupName, PrettyFormatTime(b.StartTime), b.WalFileName})
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
