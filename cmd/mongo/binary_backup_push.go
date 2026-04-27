package mongo

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
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

		pushArgs := mongo.HandleBinaryBackupPushArgs{
			Permanent:     permanent,
			SkipMetadata:  skipMetadata,
			AppName:       "wal-g-mongo " + binaryBackupPushCommandName,
			CountJournals: countJournals,
			UserDataRaw:   userDataRaw,
		}
		err := mongo.HandleBinaryBackupPush(cmd.Context(), pushArgs)
		tracelog.ErrorLogger.FatalOnError(err)
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
