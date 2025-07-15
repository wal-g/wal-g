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
	RsMemberIDsFlag              = "mongo-rs-member-ids"
	RsMemberIDsDescription       = "Comma separated integers for replica IDs of corresponding --mongo-rs-members"
	ShNameFlag                   = "mongo-sh-name"
	ShNameDescription            = "Name of shard"
	ShCfgConnStr                 = "mongo-cfg-conn-str"
	ShCfgConnStrDescription      = "Connection string to mongocfg replicas in sharded cluster"
	ShShardConnStr               = "mongo-shard-conn-str"
	ShShardConnStrDescription    = "Connection string to some shard (can be specified multiple times)"

	SkipBackupDownloadFlag        = "skip-backup-download"
	SkipBackupDownloadDescription = "Skip backup download"
	SkipChecksFlag                = "skip-checks"
	SkipChecksDescription         = "Skip checking mongod file system lock and mongo version on compatibility with backup"
	SkipMongoReconfigFlag         = "skip-mongo-reconfig"
	SkipMongoReconfigDescription  = "Skip mongo reconfiguration while restoring"
	PitrSinceFlag                 = "pitr-since"
	PitrSinceDescription          = "Timestamp point in time recovery start"
	PitrUntilFlag                 = "pitr-until"
	PitrUntilDescription          = "Timestamp point in time recovery finish"
)

var (
	minimalConfigPath        = ""
	rsName                   = ""
	rsMembers                []string
	rsMemberIDs              []int
	shardName                = ""
	mongocfgConnectionString = ""
	shardConnectionStrings   []string
	skipMongoReconfigFlag    bool
	skipBackupDownloadFlag   bool
	skipCheckFlag            bool
	pitrSince                string
	pitrUntil                string
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

		err := mongo.HandleBinaryFetchPush(ctx, mongodConfigPath, minimalConfigPath, backupName, mongodVersion,
			rsName, rsMembers, rsMemberIDs, shardName, mongocfgConnectionString, shardConnectionStrings,
			skipBackupDownloadFlag, skipMongoReconfigFlag, skipCheckFlag, pitrSince, pitrUntil)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	binaryBackupFetchCmd.Flags().StringVar(&minimalConfigPath, MinimalConfigPathFlag, "", MinimalConfigPathDescription)
	binaryBackupFetchCmd.Flags().StringVar(&rsName, RsNameFlag, "", RsNameDescription)
	binaryBackupFetchCmd.Flags().StringSliceVar(&rsMembers, RsMembersFlag, []string{}, RsMembersDescription)
	binaryBackupFetchCmd.Flags().IntSliceVar(&rsMemberIDs, RsMemberIDsFlag, []int{}, RsMemberIDsDescription)
	binaryBackupFetchCmd.Flags().StringVar(&shardName, ShNameFlag, "", ShNameDescription)
	binaryBackupFetchCmd.Flags().StringVar(&mongocfgConnectionString, ShCfgConnStr, "", ShCfgConnStrDescription)
	binaryBackupFetchCmd.Flags().StringArrayVar(&shardConnectionStrings, ShShardConnStr, []string{}, ShShardConnStrDescription)
	binaryBackupFetchCmd.Flags().BoolVar(&skipBackupDownloadFlag, SkipBackupDownloadFlag, false, SkipBackupDownloadDescription)
	binaryBackupFetchCmd.Flags().BoolVar(&skipMongoReconfigFlag, SkipMongoReconfigFlag, false, SkipMongoReconfigDescription)
	binaryBackupFetchCmd.Flags().BoolVar(&skipCheckFlag, SkipChecksFlag, false, SkipChecksDescription)
	binaryBackupFetchCmd.Flags().StringVar(&pitrSince, PitrSinceFlag, "", PitrSinceDescription)
	binaryBackupFetchCmd.Flags().StringVar(&pitrUntil, PitrUntilFlag, "", PitrUntilDescription)
	cmd.AddCommand(binaryBackupFetchCmd)
}
