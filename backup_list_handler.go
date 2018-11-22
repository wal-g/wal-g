package walg

import (
	"fmt"
	"github.com/wal-g/wal-g/tracelog"
	"os"
	"text/tabwriter"
	"time"
)

// TODO : unit tests
// HandleBackupList is invoked to perform wal-g backup-list
func HandleBackupList(folder *S3Folder) {
	backups, err := getBackups(folder)
	if err != nil {
		tracelog.ErrorLogger.FatalError(err)
	}

	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
	defer writer.Flush()
	fmt.Fprintln(writer, "name\tlast_modified\twal_segment_backup_start")

	for i := len(backups) - 1; i >= 0; i-- {
		b := backups[i]
		fmt.Fprintln(writer, fmt.Sprintf("%v\t%v\t%v", b.Name, b.Time.Format(time.RFC3339), b.WalFileName))
	}
}
