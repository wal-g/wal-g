package mongo

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/utility"
)

const (
	binaryBackupFetchCommandName = "binary-backup-fetch"
	MinimalConfigPathFlag        = "minimal-mongod-config-path"
	MinimalConfigPathDescription = "Path to mongod config with minimal working configuration"
	RsNameFlag                   = "mongo-rs-name"
	RsNameDescription            = "Name of replicaset (like rs01)"
	RsMembersFlag                = "mongo-rs-members"
	RsMembersDescription         = "Comma separated host:port records from wished rs members (like rs.initiate())"
	RsMemberIdsFlag              = "mongo-rs-member-ids"
	RsMemberIdsDescription       = "Comma separated integers for replica IDs of corresponding --mongo-rs-members"
	ShNameFlag                   = "mongo-sh-name"
	ShNameDescription            = "Name of shard"
	ShCfgConnStr                 = "mongo-cfg-conn-str"
	ShCfgConnStrDescription      = "Connection string to mongocfg replicas in shanded cluster"
	ShShardConnStr               = "mongo-shard-conn-str"
	ShShardConnStrDescription    = "Connection string to some shard (can be specified multiple times)"
)

var (
	minimalConfigPath        = ""
	rsName                   = ""
	rsMembers                []string
	rsMemberIds              []int
	shardName                = ""
	mongocfgConnectionString = ""
	shardConnectionStrings   []string
)

var binaryBackupFetchCmd = &cobra.Command{
	Use:   binaryBackupFetchCommandName + " <backup name> <mongod config path> <mongod version>",
	Short: "Fetches a mongodb binary backup from storage and restores it in mongodb storage dbPath",
	Args:  cobra.ExactArgs(3),
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		backupName := args[0]
		mongodConfigPath := args[1]
		mongodVersion := args[2]

		err := mongo.HandleBinaryFetchPush(ctx, mongodConfigPath, minimalConfigPath, backupName, mongodVersion, rsName,
			rsMembers, rsMemberIds, shardName, mongocfgConnectionString, shardConnectionStrings)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	binaryBackupFetchCmd.Flags().StringVar(&minimalConfigPath, MinimalConfigPathFlag, "", MinimalConfigPathDescription)
	binaryBackupFetchCmd.Flags().StringVar(&rsName, RsNameFlag, "", RsNameDescription)
	binaryBackupFetchCmd.Flags().StringSliceVar(&rsMembers, RsMembersFlag, []string{}, RsMembersDescription)
	binaryBackupFetchCmd.Flags().IntSliceVar(&rsMemberIds, RsMemberIdsFlag, []int{}, RsMemberIdsDescription)
	binaryBackupFetchCmd.Flags().StringVar(&shardName, ShNameFlag, "", ShNameDescription)
	binaryBackupFetchCmd.Flags().StringVar(&mongocfgConnectionString, ShCfgConnStr, "", ShCfgConnStrDescription)
	binaryBackupFetchCmd.Flags().StringArrayVar(&shardConnectionStrings, ShShardConnStr, []string{}, ShShardConnStrDescription)
	cmd.AddCommand(binaryBackupFetchCmd)
}
