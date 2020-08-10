package mongo

import (
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
)

var (
	confirmedOplogPurge bool
)

// oplogPurgeCmd represents the delete command
var oplogPurgeCmd = &cobra.Command{
	Use:   "oplog-purge",
	Short: "Purges oplog archives",
	Run:   runOplogPurge,
}

func pitrDiscoveryAfterTime() *time.Time {
	pitrDur, err := internal.GetOplogPITRDiscoveryIntervalSetting()
	tracelog.ErrorLogger.FatalOnError(err)
	if pitrDur == nil {
		return nil
	}

	pitrAfterTime := time.Now().Add(-*pitrDur)
	return &pitrAfterTime
}

func runOplogPurge(cmd *cobra.Command, args []string) {
	pitrAfterTime := pitrDiscoveryAfterTime()
	// set up storage downloader client
	downloader, err := archive.NewStorageDownloader(archive.NewDefaultStorageSettings())
	tracelog.ErrorLogger.FatalOnError(err)

	// set up storage purger client
	purger, err := archive.NewStoragePurger(archive.NewDefaultStorageSettings())
	tracelog.ErrorLogger.FatalOnError(err)

	err = mongo.HandleOplogPurge(downloader, purger, pitrAfterTime, !confirmedOplogPurge)
	tracelog.ErrorLogger.FatalOnError(err)
}

func init() {
	Cmd.AddCommand(oplogPurgeCmd)
	oplogPurgeCmd.Flags().BoolVar(&confirmedOplogPurge, internal.ConfirmFlag, false, "Confirms oplog archives deletion")
}
