package mongo

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/binary"
)

const (
	PartialFlag                             = "partial"
	PartialDescription                      = "Partial option. If this option is enabled, NamespaceNotFound errors will be ignored"
	WithCatchUpReconfigFlag                 = "with-catch-up-reconfig"
	WithCatchUpReconfigDescription          = "Reconfig MongoDB oplog service collections for mongod replica oplog catch up"
	MinimalOplogReplayConfigPathFlag        = "minimal-mongod-config-path"
	MinimalOplogReplayConfigPathDescription = "Path to mongod config with minimal working configuration"
)

var (
	partial                     bool
	withCatchUpReconfig         bool
	minimalOplogReplyConfigPath string
)

// oplogReplayCmd represents oplog replay procedure
var oplogReplayCmd = &cobra.Command{
	Use:   "oplog-replay <since ts.inc> <until ts.inc>",
	Short: "Fetches oplog archives from storage and applies to database",
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		var err error
		defer func() { tracelog.ErrorLogger.FatalOnError(err) }()

		replayArgs, mongodbURL, err := buildOplogReplayRunArgs(args, partial, withCatchUpReconfig,
			minimalOplogReplyConfigPath)
		if err != nil {
			return
		}

		err = mongo.RunOplogReplay(cmd.Context(), mongodbURL, replayArgs)
	},
}

func buildOplogReplayRunArgs(
	cmdargs []string, partial,
	withCatchUpReconfig bool, minimalConfigPath string,
) (binary.ReplyOplogConfig, string, error) {
	args, err := binary.NewReplyOplogConfig(cmd.Context(), cmdargs[0], cmdargs[1], partial, withCatchUpReconfig, minimalConfigPath)
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
	oplogReplayCmd.Flags().BoolVar(&partial, PartialFlag, false, PartialDescription)
	oplogReplayCmd.Flags().BoolVar(&withCatchUpReconfig, WithCatchUpReconfigFlag, false, WithCatchUpReconfigDescription)
	oplogReplayCmd.Flags().StringVar(&minimalOplogReplyConfigPath, MinimalOplogReplayConfigPathFlag,
		"", MinimalOplogReplayConfigPathDescription)
	cmd.AddCommand(oplogReplayCmd)
}
