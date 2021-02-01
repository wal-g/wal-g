package mongo

import (
	"context"
	"encoding/json"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/client"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/internal/databases/mongo/oplog"
	"github.com/wal-g/wal-g/internal/databases/mongo/stages"
	"github.com/wal-g/wal-g/utility"
)

// oplogReplayCmd represents oplog replay procedure
var oplogReplayCmd = &cobra.Command{
	Use:   "oplog-replay <since ts.inc> <until ts.inc>",
	Short: "Fetches oplog archives from storage and applies to database",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		defer func() { tracelog.ErrorLogger.FatalOnError(err) }()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		// resolve archiving settings
		since, err := models.TimestampFromStr(args[0])
		if err != nil {
			return
		}
		until, err := models.TimestampFromStr(args[1])
		if err != nil {
			return
		}

		// TODO: fix ugly config
		ignoreErrCodes := make(map[string][]int32)
		if ignoreErrCodesStr, ok := internal.GetSetting(internal.OplogReplayIgnoreErrorCodes); ok {
			if err = json.Unmarshal([]byte(ignoreErrCodesStr), &ignoreErrCodes); err != nil {
				return
			}
		}

		mongodbUrl, err := internal.GetRequiredSetting(internal.MongoDBUriSetting)
		if err != nil {
			return
		}

		var mongoClientArgs []client.Option
		oplogAlwaysUpsert, hasOplogAlwaysUpsert, err := internal.GetBoolSetting(internal.OplogReplayOplogAlwaysUpsert)
		if err != nil {
			return
		}
		if hasOplogAlwaysUpsert {
			mongoClientArgs = append(mongoClientArgs, client.OplogAlwaysUpsert(oplogAlwaysUpsert))
		}

		if oplogApplicationMode, hasOplogApplicationMode := internal.GetSetting(internal.OplogReplayOplogApplicationMode); hasOplogApplicationMode {
			mongoClientArgs = append(mongoClientArgs, client.OplogApplicationMode(client.OplogAppMode(oplogApplicationMode)))
		}

		err = runOplogReplay(ctx, since, until, mongodbUrl, mongoClientArgs, ignoreErrCodes)
	},
}

func runOplogReplay(ctx context.Context, since, until models.Timestamp, mongodbUrl string, mongoClientArgs []client.Option, ignoreErrCodes map[string][]int32) error {
	// set up mongodb client and oplog applier
	mongoClient, err := client.NewMongoClient(ctx, mongodbUrl, mongoClientArgs...)
	if err != nil {
		return err
	}

	if err = mongoClient.EnsureIsMaster(ctx); err != nil {
		return err
	}

	dbApplier := oplog.NewDBApplier(mongoClient, false, ignoreErrCodes)
	oplogApplier := stages.NewGenericApplier(dbApplier)

	// set up storage downloader client
	downloader, err := archive.NewStorageDownloader(archive.NewDefaultStorageSettings())
	if err != nil {
		return err
	}
	// discover archive sequence to replay
	archives, err := downloader.ListOplogArchives()
	if err != nil {
		return err
	}
	path, err := archive.SequenceBetweenTS(archives, since, until)
	if err != nil {
		return err
	}

	// setup storage fetcher
	oplogFetcher := stages.NewStorageFetcher(downloader, path)

	// run worker cycle
	return mongo.HandleOplogReplay(ctx, since, until, oplogFetcher, oplogApplier)
}

func init() {
	cmd.AddCommand(oplogReplayCmd)
}
