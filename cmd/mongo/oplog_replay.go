package mongo

import (
	"context"
	"os"
	"syscall"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

// oplogReplayCmd represents oplog replay procedure
var oplogReplayCmd = &cobra.Command{
	Use:   "oplog-replay <since ts.inc> <until ts.inc>",
	Short: "Fetches oplog archives from storage and applies to database",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		mongodbUrl, err := internal.GetRequiredSetting(internal.MongoDBUriSetting)
		tracelog.ErrorLogger.FatalOnError(err)
		oplogApplier := oplog.NewDBApplier(mongodbUrl)

		since, err := oplog.TimestampFromStr(args[0])
		tracelog.ErrorLogger.FatalOnError(err)
		until, err := oplog.TimestampFromStr(args[1])
		tracelog.ErrorLogger.FatalOnError(err)

		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		folder = folder.GetSubFolder(oplog.ArchBasePath)

		path, err := oplog.PathBetweenTS(folder, since, until)
		tracelog.ErrorLogger.FatalOnError(err)
		oplogFetcher := oplog.NewStorageFetcher(folder, path)

		err = mongo.HandleOplogReplay(ctx, since, until, oplogFetcher, oplogApplier)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(oplogReplayCmd)
}
