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
	backupPushShortDescription = "Creates new backup and pushes it to storage"
	permanentFlag              = "permanent"
	countJournalsFlag          = "count-journals"
	addUserDataFlag            = "add-user-data"

	permanentShorthand = "p"
)

var (
	// backupPushCmd represents the streamPush command
	backupPushCmd = &cobra.Command{
		Use:   "backup-push",
		Short: backupPushShortDescription,
		PreRun: func(cmd *cobra.Command, args []string) {
			conf.RequiredSettings[conf.NameStreamCreateCmd] = true
			conf.RequiredSettings[conf.MysqlDatasourceNameSetting] = true
			err := internal.AssertRequiredSettingsSet()
			tracelog.ErrorLogger.FatalOnError(err)
		},
		Run: func(cmd *cobra.Command, args []string) {
			internal.ConfigureLimiters()

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
				true,
				userData,
				mysql.NewNoDeltaBackupConfigurator(),
			)
		},
	}
	permanent     = false
	countJournals = false
	userData      = ""
)

func init() {
	cmd.AddCommand(backupPushCmd)

	// TODO: Merge similar backup-push functionality
	// to avoid code duplication in command handlers
	backupPushCmd.Flags().BoolVarP(&permanent, permanentFlag, permanentShorthand,
		false, "Pushes permanent backup")
	backupPushCmd.Flags().StringVar(&userData, addUserDataFlag,
		"", "Write the provided user data to the backup sentinel and metadata files.")
	backupPushCmd.Flags().BoolVar(&countJournals, countJournalsFlag,
		false, "Create 'backups.json' file in the bucket and maintain the binlog sizes required to get from one backup to the next one")
}
