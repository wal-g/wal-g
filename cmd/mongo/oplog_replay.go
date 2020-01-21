package mongo

import (
	"context"
	"os"
	"syscall"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"github.com/wal-g/wal-g/utility"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
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

		// resolve archiving settings
		since, err := models.TimestampFromStr(args[0])
		tracelog.ErrorLogger.FatalOnError(err)
		until, err := models.TimestampFromStr(args[1])
		tracelog.ErrorLogger.FatalOnError(err)

		mongodbUrl, err := internal.GetRequiredSetting(internal.MongoDBUriSetting)
		tracelog.ErrorLogger.FatalOnError(err)

		// set up mongodb client and oplog applier
		mongoClient, err := client.NewMongoClient(ctx, mongodbUrl)
		tracelog.ErrorLogger.FatalOnError(err)
		oplogApplier := oplog.NewDBApplier(mongoClient)

		// set up storage downloader client
		downloader, err := archive.NewStorageDownloader(models.ArchBasePath)
		tracelog.ErrorLogger.FatalOnError(err)

		// discover archive sequence to replay
		archives, err := downloader.ListOplogArchives()
		tracelog.ErrorLogger.FatalOnError(err)
		path, err := archive.SequenceBetweenTS(archives, since, until)
		tracelog.ErrorLogger.FatalOnError(err)

		// setup storage fetcher
		oplogFetcher := oplog.NewStorageFetcher(downloader, path)

		// run worker cycle
		err = mongo.HandleOplogReplay(ctx, since, until, oplogFetcher, oplogApplier)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(oplogReplayCmd)
}
