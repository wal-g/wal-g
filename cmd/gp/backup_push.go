package gp

import (
	"strconv"

	"github.com/wal-g/wal-g/internal/databases/greenplum"

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
	addUserDataFlag           = "add-user-data"

	permanentShorthand             = "p"
	fullBackupShorthand            = "f"
	verifyPagesShorthand           = "v"
	storeAllCorruptBlocksShorthand = "s"
	useRatingComposerShorthand     = "r"
)

var (
	// backupPushCmd represents the backupPush command
	backupPushCmd = &cobra.Command{
		Use:   "backup-push",
		Short: backupPushShortDescription, // TODO : improve description
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			if userDataRaw == "" {
				userDataRaw = viper.GetString(internal.SentinelUserDataSetting)
			}
			userData, err := internal.UnmarshalSentinelUserData(userDataRaw)
			tracelog.ErrorLogger.FatalfOnError("Failed to unmarshal the provided UserData: %s", err)

			logsDir := viper.GetString(internal.GPLogsDirectory)

			segPollInterval, err := internal.GetDurationSetting(internal.GPSegmentsPollInterval)
			tracelog.ErrorLogger.FatalOnError(err)

			segPollRetries := viper.GetInt(internal.GPSegmentsPollRetries)

			arguments := greenplum.NewBackupArguments(permanent, userData, prepareSegmentFwdArgs(), logsDir,
				segPollInterval, segPollRetries)
			backupHandler, err := greenplum.NewBackupHandler(arguments)
			tracelog.ErrorLogger.FatalOnError(err)
			backupHandler.HandleBackupPush()
		},
	}
	permanent   = false
	userDataRaw = ""

	// as for now, WAL-G will simply forward these arguments to the segments
	// todo: handle delta-from-name and delta-from-userdata
	fullBackup            = false
	verifyPageChecksums   = false
	storeAllCorruptBlocks = false
	useRatingComposer     = false
)

// prepare arguments that are going to be forwarded to segments
func prepareSegmentFwdArgs() []greenplum.SegmentFwdArg {
	verifyPageChecksums = verifyPageChecksums || viper.GetBool(internal.VerifyPageChecksumsSetting)
	storeAllCorruptBlocks = storeAllCorruptBlocks || viper.GetBool(internal.StoreAllCorruptBlocksSetting)
	useRatingComposer = useRatingComposer || viper.GetBool(internal.UseRatingComposerSetting)

	return []greenplum.SegmentFwdArg{
		{Name: fullBackupFlag, Value: strconv.FormatBool(fullBackup)},
		{Name: permanentFlag, Value: strconv.FormatBool(permanent)},
	}
}

func init() {
	cmd.AddCommand(backupPushCmd)

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
	backupPushCmd.Flags().StringVar(&userDataRaw, addUserDataFlag,
		"", "Write the provided user data to the backup sentinel and metadata files.")
}
