package pgbackrest

import (
	"fmt"
	"io"
	"os"
	"sort"
	"text/tabwriter"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

func HandleBackupList(folder storage.Folder, stanza string, detailed bool, pretty bool, json bool) error {
	backupTimes, err := GetBackupList(folder, stanza)

	if len(backupTimes) == 0 {
		tracelog.InfoLogger.Println("No backups found")
		return nil
	}

	if err != nil {
		return err
	}

	sort.Slice(backupTimes, func(i, j int) bool {
		return backupTimes[i].Time.Before(backupTimes[j].Time)
	})

	if detailed {
		var backupDetails []BackupDetails
		for _, backupTime := range backupTimes {
			details, err := GetBackupDetails(folder, stanza, backupTime.BackupName)
			if err != nil {
				return err
			}
			backupDetails = append(backupDetails, *details)
		}

		return printBackupListDetailed(backupDetails, pretty, json)
	}
	return printBackupList(backupTimes, pretty, json)
}

func printBackupList(backups []internal.BackupTime, pretty bool, json bool) error {
	switch {
	case json:
		return internal.WriteAsJSON(backups, os.Stdout, pretty)
	case pretty:
		internal.WritePrettyBackupList(backups, os.Stdout)
		return nil
	default:
		internal.WriteBackupList(backups, os.Stdout)
		return nil
	}
}

func printBackupListDetailed(backupDetails []BackupDetails, pretty bool, json bool) error {
	switch {
	case json:
		return internal.WriteAsJSON(backupDetails, os.Stdout, pretty)
	default:
		return writeBackupList(backupDetails, os.Stdout)
	}
}

func writeBackupList(backupDetails []BackupDetails, output io.Writer) error {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	// nolint:lll
	_, err := fmt.Fprintln(writer, "name\tmodified\twal_segment_backup_start\ttype\tstart_time\tfinish_time\tpg_version\tstart_lsn\tfinish_lsn")
	if err != nil {
		return err
	}

	for i := 0; i < len(backupDetails); i++ {
		b := backupDetails[i]
		// nolint:lll
		_, err = fmt.Fprintf(writer, "%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t%v\t\n", b.BackupName, internal.FormatTime(b.ModifiedTime), b.WalFileName, b.Type, internal.FormatTime(b.StartTime), internal.FormatTime(b.FinishTime), b.PgVersion, b.StartLsn, b.FinishLsn)

		if err != nil {
			return err
		}
	}

	return writer.Flush()
}
