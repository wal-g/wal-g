package mysql

import (
	"fmt"
	"github.com/spf13/cobra"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"os"
	"sort"
	"text/tabwriter"
	"time"
)

const backupListShortDescription = "Prints available backups"

var (
	listAll = false
)
//func MysqlHandleBackupList(folder storage.Folder) {
//	getBackupsFunc := func() ([]internal.BackupTime, error) {
//		return internal.getBackups(folder)
//	}
//	writeBackupListFunc := func(backups []internal.BackupTime) {
//		internal.WriteBackupList(backups, os.Stdout)
//	}
//	logging := Logging{
//		InfoLogger:  tracelog.InfoLogger,
//		ErrorLogger: tracelog.ErrorLogger,
//	}
//
//	HandleBackupList(getBackupsFunc, writeBackupListFunc, logging)
//}

func MysqlHandleBackupList(folder storage.Folder) {

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
			HandleListAll(folder)
		} else {
			internal.DefaultHandleBackupList(folder)
		}
	},
}
type BinlogsTime struct {
	Time       time.Time `json:"time"`
	BinlogName string    `json:"binlog_name"`
}

// Strips the backup WAL file name.
func StripBinlogsName(path string) string {
	return path
}

func BinlogTimeSlices(backups []storage.Object) []BinlogsTime {
	sortTimes := make([]BinlogsTime, len(backups))
	for i, object := range backups {
		key := object.GetName()
		time := object.GetLastModified()
		sortTimes[i] = BinlogsTime{time, StripBinlogsName(key)}
	}
	sort.Slice(sortTimes, func(i, j int) bool {
		return sortTimes[i].Time.After(sortTimes[j].Time)
	})
	return sortTimes
}

// TODO : unit tests
func Binlogs(folder storage.Folder) (backups []BinlogsTime, garbage []string, err error) {
	binlogsObjects, _, err := folder.GetSubFolder(mysql.BinlogPath).ListFolder()
	if err != nil {
		return nil, nil, err
	}

	sortTimes := BinlogTimeSlices(binlogsObjects)

	return sortTimes, garbage, nil
}

func HandleListAll(folder storage.Folder) {
	ba, _, _ := internal.GetBackupsAndGarbage(folder)

	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	defer writer.Flush()
	fmt.Fprintln(writer, "name\tlast_modified")
	for i := len(ba) - 1; i >= 0; i-- {
		b := ba[i]
		fmt.Fprintln(writer, fmt.Sprintf("%v\t%v\t", b.BackupName, b.Time.Format(time.RFC3339)))
	}

	bi, _, _ := Binlogs(folder)

	fmt.Fprintln(writer, "\n Binlogs section \n name\tlast_modified")
	for i := len(bi) - 1; i >= 0; i-- {
		b := bi[i]
		fmt.Fprintln(writer, fmt.Sprintf("%v\t%v\t", b.BinlogName, b.Time.Format(time.RFC3339)))
	}
}

func init() {
	backupListCmd.Flags().BoolVarP(&listAll, "all", "a", false, "all")
	Cmd.AddCommand(backupListCmd)
}
