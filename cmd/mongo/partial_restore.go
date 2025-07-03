package mongo

import (
	"context"
	"os"
	"strings"
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

	BlacklistFlag        = "blacklist"
	BlacklistDescription = "Comma separated dbname.colname records from wished databases " +
		"and collections which will NOT be restored partially."
)

var (
	partialMinimalConfigPath      = ""
	skipPartialCheckFlag          bool
	skipPartialBackupDownloadFlag bool
	skipPartialMongoReconfig      bool
	blacklist                     []string
)

var binaryPartialBackupFetchCmd = &cobra.Command{
	Use: binaryPartialBackupFetchCommandName +
		" <backup name> <mongod config path> <mongod version> <dbname1.colname1,dbname2,...>",
	Short: "Fetches a mongodb binary backup from storage and restores only specified " +
		"dbnames and it in mongodb storage dbPath",
	Args: cobra.ExactArgs(4),
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		backupName := args[0]
		mongodConfigPath := args[1]
		mongodVersion := args[2]
		whitelist := strings.Split(args[3], ",")

		err := mongo.HandlePartialBinaryFetch(ctx, mongodConfigPath, partialMinimalConfigPath, backupName, mongodVersion,
			skipPartialBackupDownloadFlag, skipPartialMongoReconfig, skipPartialCheckFlag,
			whitelist, blacklist)
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
	binaryPartialBackupFetchCmd.Flags().StringSliceVar(&blacklist, BlacklistFlag,
		[]string{}, BlacklistDescription)
	cmd.AddCommand(binaryPartialBackupFetchCmd)
}
