package mysql

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"io"
	"os"
	"sort"
	"text/tabwriter"
	"time"
)

const backupListShortDescription = "Prints available backups"

var (
	listAll = false
)

// TODO : unit tests
func WriteBackupList(backups []internal.BackupTime, output io.Writer) {
	writer := tabwriter.NewWriter(output, 0, 0, 1, ' ', 0)
	defer func() {
		_ = writer.Flush()
	}()

	_, _ = fmt.Fprintln(writer, "name\tlast_modified\twal_segment_backup_start")
	for i := len(backups) - 1; i >= 0; i-- {
		b := backups[i]
		_, _ = fmt.Fprintln(writer, fmt.Sprintf("%v\t%v\t%v", b.BackupName, b.Time.Format(time.RFC3339), b.WalFileName))
	}
}

func HandleBackupList(folder storage.Folder) {
	getBackupsFunc := func() ([]internal.BackupTime, error) {
		return internal.Backups(folder)
	}
	writeBackupListFunc := func(backups []internal.BackupTime) {
		WriteBackupList(backups, os.Stdout)
	}

	logging := internal.Logging{
		InfoLogger:  tracelog.InfoLogger,
		ErrorLogger: tracelog.ErrorLogger,
	}

	internal.HandleBackupList(getBackupsFunc, writeBackupListFunc, logging)
}

// backupListCmd represents the backupList command
var backupListCmd = &cobra.Command{
	Use:   "backup-list",
	Short: backupListShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		if listAll {
			err := HandleListAll(folder)
			if err != nil {
				tracelog.ErrorLogger.FatalError(err)
			}
			return
		}

		HandleBackupList(folder)
	},
}

// Strips the backup WAL file name.
func StripBinlogsName(path string) string {
	return path
}

func BinlogTimeSlices(backups []storage.Object) []mysql.BinlogsTime {
	sortTimes := make([]mysql.BinlogsTime, len(backups))
	for i, object := range backups {
		sortTimes[i] = mysql.BinlogsTime{
			Time:       object.GetLastModified(),
			BinlogName: StripBinlogsName(object.GetName()),
		}
	}
	sort.Slice(sortTimes, func(i, j int) bool {
		return sortTimes[i].Time.After(sortTimes[j].Time)
	})
	return sortTimes
}

// TODO : unit tests
func binlogs(folder storage.Folder) (backups []mysql.BinlogsTime, garbage []string, err error) {
	binlogsObjects, _, err := folder.GetSubFolder(mysql.BinlogPath).ListFolder()
	if err != nil {
		return nil, nil, err
	}

	sortTimes := BinlogTimeSlices(binlogsObjects)

	return sortTimes, garbage, nil
}

func HandleListAll(folder storage.Folder) error {
	ba, _, err := internal.BackupsAndGarbage(folder)
	if err != nil {
		return err
	}

	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	defer func() {
		_ = writer.Flush()
	}()

	_, err = fmt.Fprintln(writer, "name\tlast_modified")
	if err != nil {
		return err
	}

	for i := len(ba) - 1; i >= 0; i-- {
		b := ba[i]
		_, err = fmt.Fprintln(writer, fmt.Sprintf("%v\t%v\t", b.BackupName, b.Time.Format(time.RFC3339)))
		if err != nil {
			return err
		}
	}

	bi, _, err := binlogs(folder)
	if err != nil {
		return err
	}

	_, _ = fmt.Fprintln(writer, "\nBinlogs section \n\nName\tlast_modified")
	for i := len(bi) - 1; i >= 0; i-- {
		b := bi[i]
		_, _ = fmt.Fprintln(writer, fmt.Sprintf("%v\t%v\t", b.BinlogName, b.Time.Format(time.RFC3339)))
	}

	return nil
}

func init() {
	backupListCmd.Flags().BoolVarP(&listAll, "all", "a", false, "all")
	Cmd.AddCommand(backupListCmd)
}
