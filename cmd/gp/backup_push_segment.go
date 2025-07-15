package gp

import (
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
)

const (
	segBackupPushShortDescription = "Makes a segment backup and uploads it to storage"
)

var (
	// segBackupPushCmd is a subcommand to make a backup of a single segment.
	// It is called remotely by a backup-push command from the master host
	segBackupPushCmd = &cobra.Command{
		Use:   "seg-backup-push db_directory --content-id=[content_id]",
		Short: segBackupPushShortDescription, // TODO : improve description
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			internal.ConfigureLimiters()

			greenplum.SetSegmentStoragePrefix(contentID)

			rootFolder, err := getMultistorageRootFolder(true, policies.TakeFirstStorage)
			tracelog.ErrorLogger.FatalOnError(err)

			uploader, err := internal.ConfigureUploaderToFolder(rootFolder)
			tracelog.ErrorLogger.FatalOnError(err)

			dataDirectory := args[0]

			if deltaFromName == "" {
				deltaFromName = viper.GetString(conf.DeltaFromNameSetting)
			}
			if deltaFromUserData == "" {
				deltaFromUserData = viper.GetString(conf.DeltaFromUserDataSetting)
			}
			if userDataRaw == "" {
				userDataRaw = viper.GetString(conf.SentinelUserDataSetting)
			}

			if deltaFromName == "" && deltaFromUserData == "" {
				fullBackup = true
			}
			deltaBaseSelector, err := internal.NewDeltaBaseSelector(
				deltaFromName, deltaFromUserData, postgres.NewGenericMetaFetcher())
			tracelog.ErrorLogger.FatalOnError(err)

			userData, err := internal.UnmarshalSentinelUserData(userDataRaw)
			tracelog.ErrorLogger.FatalfOnError("Failed to unmarshal the provided UserData: %s", err)

			// currently, these features are not supported
			verifyPageChecksums := false
			storeAllCorruptBlocks := false
			tarBallComposerType := postgres.RegularComposer

			arguments := postgres.NewBackupArguments(uploader, dataDirectory, utility.BaseBackupPath,
				permanent, verifyPageChecksums,
				fullBackup, storeAllCorruptBlocks,
				tarBallComposerType, greenplum.NewSegDeltaBackupConfigurator(deltaBaseSelector),
				userData, viper.GetBool(conf.WithoutFilesMetadataSetting))

			backupHandler, err := greenplum.NewSegBackupHandler(arguments)
			tracelog.ErrorLogger.FatalOnError(err)
			backupHandler.HandleBackupPush(cmd.Context())
		},
	}
)

func init() {
	// Since this is a utility command, it should not be exposed to the end user.
	segBackupPushCmd.Hidden = true
	segBackupPushCmd.Flags().BoolVarP(&permanent, permanentFlag, permanentShorthand,
		false, "Pushes permanent backup")
	segBackupPushCmd.Flags().BoolVarP(&fullBackup, fullBackupFlag, fullBackupShorthand,
		false, "Make full backup-push")
	segBackupPushCmd.Flags().StringVar(&deltaFromName, deltaFromNameFlag,
		"", "Select the backup specified by name as the target for the delta backup")
	segBackupPushCmd.Flags().StringVar(&deltaFromUserData, deltaFromUserDataFlag,
		"", "Select the backup specified by UserData as the target for the delta backup")
	segBackupPushCmd.Flags().StringVar(&userDataRaw, addUserDataFlag,
		"", "Write the provided user data to the backup sentinel and metadata files.")
	segBackupPushCmd.PersistentFlags().IntVar(&contentID, "content-id", 0, "segment content ID")
	_ = segBackupPushCmd.MarkFlagRequired("content-id")
	cmd.AddCommand(segBackupPushCmd)
}
