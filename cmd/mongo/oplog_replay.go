package mongo

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
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

		replayArgs, mongodbURL, err := buildOplogReplayRunArgs(args)
		if err != nil {
			return
		}

		err = mongo.RunOplogReplay(ctx, mongodbURL, replayArgs)
	},
}

func buildOplogReplayRunArgs(cmdargs []string) (binary.ReplyOplogConfig, string, error) {
	args, err := binary.NewReplyOplogConfig(cmdargs[0], cmdargs[1])
	if err != nil {
		return args, "", err
	}

	mongodbURL, err := conf.GetRequiredSetting(conf.MongoDBUriSetting)
	if err != nil {
		return args, "", err
	}

	return args, mongodbURL, nil
}

func init() {
	cmd.AddCommand(oplogReplayCmd)
}
