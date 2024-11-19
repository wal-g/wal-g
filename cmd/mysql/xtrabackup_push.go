package mysql

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/utility"
)

const (
	xtrabackupPushShortDescription = "Creates new backup and pushes it to storage"

	fullBackupFlag        = "full"
	deltaFromUserDataFlag = "delta-from-user-data"
	deltaFromNameFlag     = "delta-from-name"

	fullBackupShorthand = "f"
)

var (
	// backupPushCmd represents the streamPush command
	xtrabackupPushCmd = &cobra.Command{
		Use:   "xtrabackup-push",
		Short: xtrabackupPushShortDescription,
		PreRun: func(cmd *cobra.Command, args []string) {
			conf.RequiredSettings[conf.NameStreamCreateCmd] = true
			conf.RequiredSettings[conf.MysqlDatasourceNameSetting] = true
			err := internal.AssertRequiredSettingsSet()
			tracelog.ErrorLogger.FatalOnError(err)
		},
		Run: func(cmd *cobra.Command, args []string) {
			internal.ConfigureLimiters()

			// FIXME: do we need this?
			if deltaFromName == "" {
				deltaFromName = viper.GetString(conf.DeltaFromNameSetting)
			}
			if deltaFromUserData == "" {
				deltaFromUserData = viper.GetString(conf.DeltaFromUserDataSetting)
			}

			deltaBaseSelector, err := internal.NewDeltaBaseSelector(
				deltaFromName, deltaFromUserData, mysql.NewGenericMetaFetcher())
			tracelog.ErrorLogger.FatalOnError(err)

			uploader, err := internal.ConfigureSplitUploader()
			tracelog.ErrorLogger.FatalOnError(err)
			folder := uploader.Folder()
			uploader.ChangeDirectory(utility.BaseBackupPath)
			backupCmd, err := internal.GetCommandSetting(conf.NameStreamCreateCmd)
			tracelog.ErrorLogger.FatalOnError(err)

			if userData == "" {
				userData = viper.GetString(conf.SentinelUserDataSetting)
			}

			mysql.HandleBackupPush(
				folder,
				uploader,
				backupCmd,
				permanent,
				countJournals,
				fullBackup,
				userData,
				mysql.NewRegularDeltaBackupConfigurator(folder, deltaBaseSelector),
			)
		},
	}
	fullBackup        = true
	deltaFromName     = ""
	deltaFromUserData = ""
)

func init() {
	cmd.AddCommand(xtrabackupPushCmd)

	// TODO: Merge similar backup-push functionality
	// to avoid code duplication in command handlers
	xtrabackupPushCmd.Flags().BoolVarP(&permanent, permanentFlag, permanentShorthand,
		false, "Pushes permanent backup")
	xtrabackupPushCmd.Flags().BoolVarP(&fullBackup, fullBackupFlag, fullBackupShorthand,
		true, "Make full backup-push")
	xtrabackupPushCmd.Flags().StringVar(&deltaFromName, deltaFromNameFlag,
		"", "Select the backup specified by name as the target for the delta backup")
	xtrabackupPushCmd.Flags().StringVar(&deltaFromUserData, deltaFromUserDataFlag,
		"", "Select the backup specified by UserData as the target for the delta backup")
	xtrabackupPushCmd.Flags().StringVar(&userData, addUserDataFlag,
		"", "Write the provided user data to the backup sentinel and metadata files.")
	xtrabackupPushCmd.Flags().BoolVar(&countJournals, countJournalsFlag,
		false, "Create 'backups.json' file in the bucket and maintain the binlog sizes required to get from one backup to the next one")
}
