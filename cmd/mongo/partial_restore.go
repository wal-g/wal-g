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
	binaryPartialBackupFetchCommandName = "partial-restore"
	PartialMinimalConfigPathFlag        = "partial-minimal-mongod-config-path"
	PartialMinimalConfigPathDescription = "Path to mongod config with minimal working configuration"

	SkipPartialBackupDownloadFlag        = "skip-partial-backup-download"
	SkipPartialBackupDownloadDescription = "Skip backup download"
	SkipPartialChecksFlag                = "skip-partial-checks"
	SkipPartialChecksDescription         = "Skip checking mongod file system lock and mongo version on compatibility " +
		"with backup"
	SkipPartialMongoReconfigFlag        = "skip-partial-mongo-reconfig"
	SkipPartialMongoReconfigDescription = "Skip mongo reconfiguration while restoring"

	WhitelistFlag        = "whitelist"
	WhitelistDescription = "Comma separated dbname.colname records from wished databases " +
		"and collections restored partially. Indexes included"
	BlacklistFlag        = "blacklist"
	BlacklistDescription = "Comma separated dbname.colname records from wished databases " +
		"and collections NOT restored partially."
	PartiallyRestoreWithSystemDBsFlag        = "with-system-dbs"
	PartiallyRestoreWithSystemDBsDescription = "Always restore 'admin' and 'local' dbs in partially restore. " +
		"Restore 'config' also if rs-name flag is set"
)

var (
	partialMinimalConfigPath      = ""
	skipPartialCheckFlag          bool
	skipPartialBackupDownloadFlag bool
	skipPartialMongoReconfig      bool
	whitelist                     []string
	blacklist                     []string
	partiallyRestoreWithSystemDBs bool
)

var binaryPartialBackupFetchCmd = &cobra.Command{
	Use:   binaryPartialBackupFetchCommandName + " <backup name> <mongod config path> <mongod version>",
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

		err := mongo.HandlePartialBinaryFetch(ctx, mongodConfigPath, partialMinimalConfigPath, backupName, mongodVersion,
			skipPartialBackupDownloadFlag, skipPartialMongoReconfig, skipPartialCheckFlag,
			whitelist, blacklist, partiallyRestoreWithSystemDBs)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	binaryPartialBackupFetchCmd.Flags().StringVar(&partialMinimalConfigPath, PartialMinimalConfigPathFlag, "",
		PartialMinimalConfigPathDescription)
	binaryPartialBackupFetchCmd.Flags().BoolVar(&skipPartialBackupDownloadFlag, SkipPartialBackupDownloadFlag,
		false, SkipPartialBackupDownloadDescription)
	binaryPartialBackupFetchCmd.Flags().BoolVar(&skipPartialMongoReconfig, SkipPartialMongoReconfigFlag,
		false, SkipPartialMongoReconfigDescription)
	binaryPartialBackupFetchCmd.Flags().BoolVar(&skipPartialCheckFlag, SkipPartialChecksFlag,
		false, SkipPartialChecksDescription)
	binaryPartialBackupFetchCmd.Flags().StringSliceVar(&whitelist, WhitelistFlag,
		[]string{}, WhitelistDescription)
	binaryPartialBackupFetchCmd.Flags().StringSliceVar(&blacklist, BlacklistFlag,
		[]string{}, BlacklistDescription)
	binaryPartialBackupFetchCmd.Flags().BoolVar(&partiallyRestoreWithSystemDBs, PartiallyRestoreWithSystemDBsFlag,
		false, PartiallyRestoreWithSystemDBsDescription)
	cmd.AddCommand(binaryPartialBackupFetchCmd)
}
