package mongo

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
)

const (
	retainAfterFlag  = "retain-after"
	retainCountFlag  = "retain-count"
	purgeOplogFlag   = "purge-oplog"
	purgeGarbageFlag = "purge-garbage"
)

var (
	confirmed    bool
	purgeOplog   bool
	purgeGarbage bool
	retainAfter  string
	retainCount  uint
)

// deleteCmd represents the delete command
var deleteCmd = &cobra.Command{
	Use:   "delete",
	Short: "Clears old backups and oplog",
	Run:   runPurge,
}

func runPurge(cmd *cobra.Command, args []string) {
	opts := []mongo.PurgeOption{mongo.PurgeDryRun(!confirmed), mongo.PurgeOplog(purgeOplog), mongo.PurgeGarbage(purgeGarbage)}
	if cmd.Flags().Changed(retainAfterFlag) {
		retainAfterTime, err := time.Parse(time.RFC3339, retainAfter)
		tracelog.ErrorLogger.FatalfOnError("Can not parse retain time: %v", err)
		opts = append(opts, mongo.PurgeRetainAfter(retainAfterTime))
	} else if cmd.Flags().Changed(purgeOplogFlag) {
		tracelog.ErrorLogger.Fatalf("Flag %q requires %q to be passed\n", purgeOplogFlag, retainAfterFlag)
	}

	if cmd.Flags().Changed(retainCountFlag) {
		if retainCount == 0 { // TODO: provide folder cleanup
			tracelog.ErrorLogger.Fatalln("Retain count can not be 0")
		}
		opts = append(opts, mongo.PurgeRetainCount(int(retainCount)))
	}

	// set up storage downloader client
	downloader, err := archive.NewStorageDownloader(archive.NewDefaultStorageSettings())
	tracelog.ErrorLogger.FatalOnError(err)

	// set up storage downloader client
	purger, err := archive.NewStoragePurger(archive.NewDefaultStorageSettings())
	tracelog.ErrorLogger.FatalOnError(err)

	err = mongo.HandlePurge(downloader, purger, opts...)
	tracelog.ErrorLogger.FatalOnError(err)

}

func init() {
	cmd.AddCommand(deleteCmd)
	deleteCmd.Flags().BoolVar(&confirmed, internal.ConfirmFlag, false, "Confirms backup deletion")
	deleteCmd.Flags().BoolVar(&purgeOplog, purgeOplogFlag, false, "Purge oplog archives")
	deleteCmd.Flags().BoolVar(&purgeGarbage, purgeGarbageFlag, false, "Purge garbage in backup folder")
	deleteCmd.Flags().StringVar(&retainAfter, retainAfterFlag, "", "Keep backups newer")
	deleteCmd.Flags().UintVar(&retainCount, retainCountFlag, 0, "Keep minimum count, except permanent backups")
}
