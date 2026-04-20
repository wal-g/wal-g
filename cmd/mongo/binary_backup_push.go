package mongo

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/logging"
	"github.com/wal-g/wal-g/utility"
)

const (
	binaryBackupPushCommandName = "binary-backup-push"

	SkipMetadataFlag  = "skip-metadata"
	CountJournalsFlag = "count-journals"
	AddUserDataFlag   = "add-user-data"
)

var (
	countJournals = false
	skipMetadata  = false
	userDataRaw   = ""
)

var binaryBackupPushCmd = &cobra.Command{
	Use:   binaryBackupPushCommandName,
	Short: "Creates mongodb binary backup and pushes it to storage without local disk",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		pushArgs := mongo.HandleBinaryBackupPushArgs{
			Permanent:     permanent,
			SkipMetadata:  skipMetadata,
			AppName:       "wal-g-mongo " + binaryBackupPushCommandName,
			CountJournals: countJournals,
			UserDataRaw:   userDataRaw,
		}
		err := mongo.HandleBinaryBackupPush(ctx, pushArgs)
		logging.FatalOnError(err)
	},
}

func init() {
	binaryBackupPushCmd.Flags().BoolVarP(&permanent, PermanentFlag, PermanentShorthand, false, "Pushes permanent backup")
	binaryBackupPushCmd.Flags().BoolVar(&skipMetadata, SkipMetadataFlag, false, "Skip metadata collecting for partial restore")
	binaryBackupPushCmd.Flags().BoolVar(&countJournals, CountJournalsFlag, false,
		"Count and store in S3 oplog sizes required to get replay data from a backup to the next one")
	binaryBackupPushCmd.Flags().StringVar(&userDataRaw, AddUserDataFlag,
		"", "Write the provided user data to the backup sentinel and metadata files.")
	cmd.AddCommand(binaryBackupPushCmd)
}
