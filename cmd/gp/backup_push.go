package gp

import (
	"strconv"

	"github.com/wal-g/wal-g/internal/databases/greenplum"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
)

const (
	backupPushShortDescription = "Makes backup and uploads it to storage"

	permanentFlag         = "permanent"
	fullBackupFlag        = "full"
	addUserDataFlag       = "add-user-data"
	deltaFromUserDataFlag = "delta-from-user-data"
	deltaFromNameFlag     = "delta-from-name"

	permanentShorthand  = "p"
	fullBackupShorthand = "f"
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
				deltaFromName = viper.GetString(conf.DeltaFromNameSetting)
			}
			if deltaFromUserData == "" {
				deltaFromUserData = viper.GetString(conf.DeltaFromUserDataSetting)
			}
			if userDataRaw == "" {
				userDataRaw = viper.GetString(conf.SentinelUserDataSetting)
			}
			userData, err := internal.UnmarshalSentinelUserData(userDataRaw)
			tracelog.ErrorLogger.FatalfOnError("Failed to unmarshal the provided UserData: %s", err)

			deltaBaseSelector, err := internal.NewDeltaBaseSelector(
				deltaFromName, deltaFromUserData, greenplum.NewGenericMetaFetcher())
			tracelog.ErrorLogger.FatalfOnError("Failed to find the base for a delta backup: %s", err)

			logsDir := viper.GetString(conf.GPLogsDirectory)

			segPollInterval, err := conf.GetDurationSetting(conf.GPSegmentsPollInterval)
			tracelog.ErrorLogger.FatalOnError(err)

			segPollRetries := viper.GetInt(conf.GPSegmentsPollRetries)

			rootFolder, err := getMultistorageRootFolder(true, policies.TakeFirstStorage)
			tracelog.ErrorLogger.FatalOnError(err)

			uploader, err := internal.ConfigureUploaderToFolder(rootFolder)
			tracelog.ErrorLogger.FatalOnError(err)

			arguments := greenplum.NewBackupArguments(uploader, permanent, fullBackup, userData, prepareSegmentFwdArgs(), logsDir,
				segPollInterval, segPollRetries, deltaBaseSelector)
			backupHandler, err := greenplum.NewBackupHandler(arguments)
			tracelog.ErrorLogger.FatalOnError(err)
			backupHandler.HandleBackupPush()
		},
	}
	permanent   = false
	userDataRaw = ""

	deltaFromName     = ""
	deltaFromUserData = ""
	fullBackup        = false
)

// prepare arguments that are going to be forwarded to segments
func prepareSegmentFwdArgs() []greenplum.SegmentFwdArg {
	return []greenplum.SegmentFwdArg{
		{Name: permanentFlag, Value: strconv.FormatBool(permanent)},
	}
}

func init() {
	cmd.AddCommand(backupPushCmd)

	backupPushCmd.Flags().BoolVarP(&permanent, permanentFlag, permanentShorthand,
		false, "Pushes permanent backup")
	backupPushCmd.Flags().BoolVarP(&fullBackup, fullBackupFlag, fullBackupShorthand,
		false, "Make full backup-push")
	backupPushCmd.Flags().StringVar(&userDataRaw, addUserDataFlag,
		"", "Write the provided user data to the backup sentinel and metadata files.")
	backupPushCmd.Flags().StringVar(&deltaFromName, deltaFromNameFlag,
		"", "Select the backup specified by name as the target for the delta backup")
	backupPushCmd.Flags().StringVar(&deltaFromUserData, deltaFromUserDataFlag,
		"", "Select the backup specified by UserData as the target for the delta backup")
}
