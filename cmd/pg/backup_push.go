package pg

import (
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/utility"

	"github.com/wal-g/wal-g/internal/databases/postgres"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	backupPushShortDescription = "Makes backup and uploads it to storage"

	permanentFlag             = "permanent"
	fullBackupFlag            = "full"
	verifyPagesFlag           = "verify"
	storeAllCorruptBlocksFlag = "store-all-corrupt"
	useRatingComposerFlag     = "rating-composer"
	useCopyComposerFlag       = "copy-composer"
	useDatabaseComposerFlag   = "database-composer"
	deltaFromUserDataFlag     = "delta-from-user-data"
	deltaFromNameFlag         = "delta-from-name"
	addUserDataFlag           = "add-user-data"
	withoutFilesMetadataFlag  = "without-files-metadata"

	permanentShorthand             = "p"
	fullBackupShorthand            = "f"
	verifyPagesShorthand           = "v"
	storeAllCorruptBlocksShorthand = "s"
	useRatingComposerShorthand     = "r"
	useCopyComposerShorthand       = "c"
	useDatabaseComposerShorthand   = "b"
)

var (
	// backupPushCmd represents the backupPush command
	backupPushCmd = &cobra.Command{
		Use:   "backup-push db_directory",
		Short: backupPushShortDescription, // TODO : improve description
		Args:  cobra.MaximumNArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			internal.ConfigureLimiters()

			baseUploader, err := internal.ConfigureUploader()
			tracelog.ErrorLogger.FatalOnError(err)

			failover, err := internal.InitFailoverStorages()
			tracelog.ErrorLogger.FatalOnError(err)

			uploader, err := multistorage.NewUploader(baseUploader, failover)
			tracelog.ErrorLogger.FatalOnError(err)

			var dataDirectory string

			if len(args) > 0 {
				dataDirectory = args[0]
			}

			verifyPageChecksums = verifyPageChecksums || viper.GetBool(internal.VerifyPageChecksumsSetting)
			storeAllCorruptBlocks = storeAllCorruptBlocks || viper.GetBool(internal.StoreAllCorruptBlocksSetting)

			tarBallComposerType := chooseTarBallComposer()

			if deltaFromName == "" {
				deltaFromName = viper.GetString(internal.DeltaFromNameSetting)
			}
			if deltaFromUserData == "" {
				deltaFromUserData = viper.GetString(internal.DeltaFromUserDataSetting)
			}
			if userDataRaw == "" {
				userDataRaw = viper.GetString(internal.SentinelUserDataSetting)
			}
			withoutFilesMetadata = withoutFilesMetadata || viper.GetBool(internal.WithoutFilesMetadataSetting)
			if withoutFilesMetadata {
				// files metadata tracking is required for delta backups and copy/rating composers
				if tarBallComposerType != postgres.RegularComposer {
					tracelog.ErrorLogger.Fatalf(
						"%s option cannot be used with non-regular tar ball composer",
						withoutFilesMetadataFlag)
				}
				if deltaFromName != "" || deltaFromUserData != "" {
					tracelog.ErrorLogger.Fatalf(
						"%s option cannot be used with %s, %s options",
						withoutFilesMetadataFlag, deltaFromNameFlag, deltaFromUserDataFlag)
				}
				tracelog.InfoLogger.Print("Files metadata tracking is disabled")
				fullBackup = true
			}

			deltaBaseSelector, err := internal.NewDeltaBaseSelector(
				deltaFromName, deltaFromUserData, postgres.NewGenericMetaFetcher())
			tracelog.ErrorLogger.FatalOnError(err)

			userData, err := internal.UnmarshalSentinelUserData(userDataRaw)
			tracelog.ErrorLogger.FatalfOnError("Failed to unmarshal the provided UserData: %s", err)

			arguments := postgres.NewBackupArguments(uploader, dataDirectory, utility.BaseBackupPath,
				permanent, verifyPageChecksums || viper.GetBool(internal.VerifyPageChecksumsSetting),
				fullBackup, storeAllCorruptBlocks || viper.GetBool(internal.StoreAllCorruptBlocksSetting),
				tarBallComposerType, postgres.NewRegularDeltaBackupConfigurator(deltaBaseSelector),
				userData, withoutFilesMetadata)

			backupHandler, err := postgres.NewBackupHandler(arguments)
			tracelog.ErrorLogger.FatalOnError(err)
			backupHandler.HandleBackupPush()
		},
	}
	permanent             = false
	fullBackup            = false
	verifyPageChecksums   = false
	storeAllCorruptBlocks = false
	useRatingComposer     = false
	useDatabaseComposer   = false
	useCopyComposer       = false
	deltaFromName         = ""
	deltaFromUserData     = ""
	userDataRaw           = ""
	withoutFilesMetadata  = false
)

func chooseTarBallComposer() postgres.TarBallComposerType {
	tarBallComposerType := postgres.RegularComposer

	useRatingComposer = useRatingComposer || viper.GetBool(internal.UseRatingComposerSetting)
	if useRatingComposer {
		tarBallComposerType = postgres.RatingComposer
	}

	useDatabaseComposer = useDatabaseComposer || viper.GetBool(internal.UseDatabaseComposerSetting)
	if useDatabaseComposer {
		tarBallComposerType = postgres.DatabaseComposer
	}

	useCopyComposer = useCopyComposer || viper.GetBool(internal.UseCopyComposerSetting)
	if useCopyComposer {
		fullBackup = true
		tarBallComposerType = postgres.CopyComposer
	}

	return tarBallComposerType
}

func init() {
	Cmd.AddCommand(backupPushCmd)

	backupPushCmd.Flags().BoolVarP(&permanent, permanentFlag, permanentShorthand,
		false, "Pushes permanent backup")
	backupPushCmd.Flags().BoolVarP(&fullBackup, fullBackupFlag, fullBackupShorthand,
		false, "Make full backup-push")
	backupPushCmd.Flags().BoolVarP(&verifyPageChecksums, verifyPagesFlag, verifyPagesShorthand,
		false, "Verify page checksums")
	backupPushCmd.Flags().BoolVarP(&storeAllCorruptBlocks, storeAllCorruptBlocksFlag, storeAllCorruptBlocksShorthand,
		false, "Store all corrupt blocks found during page checksum verification")
	backupPushCmd.Flags().BoolVarP(&useRatingComposer, useRatingComposerFlag, useRatingComposerShorthand,
		false, "Use rating tar composer (beta)")
	backupPushCmd.Flags().BoolVarP(&useCopyComposer, useCopyComposerFlag, useCopyComposerShorthand,
		false, "Use copy tar composer (beta)")
	backupPushCmd.Flags().BoolVarP(&useDatabaseComposer, useDatabaseComposerFlag, useDatabaseComposerShorthand,
		false, "Use database tar composer (experimental)")
	backupPushCmd.Flags().StringVar(&deltaFromName, deltaFromNameFlag,
		"", "Select the backup specified by name as the target for the delta backup")
	backupPushCmd.Flags().StringVar(&deltaFromUserData, deltaFromUserDataFlag,
		"", "Select the backup specified by UserData as the target for the delta backup")
	backupPushCmd.Flags().StringVar(&userDataRaw, addUserDataFlag,
		"", "Write the provided user data to the backup sentinel and metadata files.")
	backupPushCmd.Flags().BoolVar(&withoutFilesMetadata, withoutFilesMetadataFlag,
		false, "Do not track files metadata, significantly reducing memory usage")
}
