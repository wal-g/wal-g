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

	permanentFlag           = "permanent"
	fullBackupFlag          = "full"
	addUserDataFlag         = "add-user-data"
	deltaFromUserDataFlag   = "delta-from-user-data"
	deltaFromNameFlag       = "delta-from-name"
	useDatabaseComposerFlag = "database-composer"

	permanentShorthand           = "p"
	fullBackupShorthand          = "f"
	useDatabaseComposerShorthand = "b"
)

var (
	// backupPushCmd represents the backupPush command
	backupPushCmd = &cobra.Command{
		Use:   "backup-push",
		Short: backupPushShortDescription, // TODO : improve description
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			internal.ConfigureLimiters()

			if deltaFromName == "" {
				deltaFromName = viper.GetString(internal.DeltaFromNameSetting)
			}
			if deltaFromUserData == "" {
				deltaFromUserData = viper.GetString(internal.DeltaFromUserDataSetting)
			}
			if userDataRaw == "" {
				userDataRaw = viper.GetString(internal.SentinelUserDataSetting)
			}
			userData, err := internal.UnmarshalSentinelUserData(userDataRaw)
			tracelog.ErrorLogger.FatalfOnError("Failed to unmarshal the provided UserData: %s", err)

			deltaBaseSelector, err := internal.NewDeltaBaseSelector(
				deltaFromName, deltaFromUserData, greenplum.NewGenericMetaFetcher())
			tracelog.ErrorLogger.FatalfOnError("Failed to find the base for a delta backup: %s", err)

			logsDir := viper.GetString(internal.GPLogsDirectory)

			segPollInterval, err := internal.GetDurationSetting(internal.GPSegmentsPollInterval)
			tracelog.ErrorLogger.FatalOnError(err)

			segPollRetries := viper.GetInt(internal.GPSegmentsPollRetries)

			arguments := greenplum.NewBackupArguments(permanent, fullBackup, userData, prepareSegmentFwdArgs(), logsDir,
				segPollInterval, segPollRetries, deltaBaseSelector)
			backupHandler, err := greenplum.NewBackupHandler(arguments)
			tracelog.ErrorLogger.FatalOnError(err)
			backupHandler.HandleBackupPush()
		},
	}
	permanent           = false
	userDataRaw         = ""
	useDatabaseComposer = false
	deltaFromName       = ""
	deltaFromUserData   = ""
	fullBackup          = false
)

// prepare arguments that are going to be forwarded to segments
func prepareSegmentFwdArgs() []greenplum.SegmentFwdArg {
	return []greenplum.SegmentFwdArg{
		{Name: permanentFlag, Value: strconv.FormatBool(permanent)},
		{Name: useDatabaseComposerFlag, Value: strconv.FormatBool(useDatabaseComposer)},
	}
}

func init() {
	cmd.AddCommand(backupPushCmd)

	backupPushCmd.Flags().BoolVarP(&permanent, permanentFlag, permanentShorthand,
		false, "Pushes permanent backup")
	backupPushCmd.Flags().BoolVarP(&fullBackup, fullBackupFlag, fullBackupShorthand,
		false, "Make full backup-push")
	backupPushCmd.Flags().BoolVarP(&useDatabaseComposer, useDatabaseComposerFlag, useDatabaseComposerShorthand,
		false, "Use database tar composer (experimental)")
	backupPushCmd.Flags().StringVar(&userDataRaw, addUserDataFlag,
		"", "Write the provided user data to the backup sentinel and metadata files.")
	backupPushCmd.Flags().StringVar(&deltaFromName, deltaFromNameFlag,
		"", "Select the backup specified by name as the target for the delta backup")
	backupPushCmd.Flags().StringVar(&deltaFromUserData, deltaFromUserDataFlag,
		"", "Select the backup specified by UserData as the target for the delta backup")
}
