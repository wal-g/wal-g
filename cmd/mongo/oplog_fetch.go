package mongo

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
	"github.com/wal-g/wal-g/utility"
)

var (
	format string
)

// oplogFetchCmd represents oplog replay procedure
var oplogFetchCmd = &cobra.Command{
	Use:   "oplog-fetch <since ts.inc> <until ts.inc>",
	Short: "Fetches oplog archives from storage and dumps to stdout",
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

		formatApplier, err := oplog.NewWriteApplier(format, os.Stdout)
		tracelog.ErrorLogger.FatalOnError(err)
		oplogApplier := stages.NewGenericApplier(formatApplier)

		// set up storage downloader client
		downloader, err := archive.NewStorageDownloader(archive.NewDefaultStorageSettings())
		tracelog.ErrorLogger.FatalOnError(err)

		// discover archive sequence to replay
		archives, err := downloader.ListOplogArchives()
		tracelog.ErrorLogger.FatalOnError(err)
		path, err := archive.SequenceBetweenTS(archives, since, until)
		tracelog.ErrorLogger.FatalOnError(err)

		// setup storage fetcher
		oplogFetcher := stages.NewStorageFetcher(downloader, path)

		// run worker cycle
		err = mongo.HandleOplogReplay(ctx, since, until, oplogFetcher, oplogApplier)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	cmd.AddCommand(oplogFetchCmd)
	oplogFetchCmd.PersistentFlags().StringVarP(
		&format, "format", "f", "json", "Valid values: json, bson, bson-raw")
}
