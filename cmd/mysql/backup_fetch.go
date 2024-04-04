package mysql

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mysql"
)

const (
	backupFetchShortDescription = "Fetch desired backup from storage"
	targetUserDataDescription   = "Fetch storage backup which has the specified user data"
)

var (
	// backupFetchCmd represents the streamFetch command
	backupFetchCmd = &cobra.Command{
		Use:   "backup-fetch backup-name",
		Short: backupFetchShortDescription,
		Args:  cobra.RangeArgs(0, 1),
		PreRun: func(cmd *cobra.Command, args []string) {
			conf.RequiredSettings[conf.NameStreamRestoreCmd] = true
			err := internal.AssertRequiredSettingsSet()
			tracelog.ErrorLogger.FatalOnError(err)
		},
		Run: func(cmd *cobra.Command, args []string) {
			internal.ConfigureLimiters()
			storage, err := internal.ConfigureStorage()
			tracelog.ErrorLogger.FatalOnError(err)
			restoreCmd, err := internal.GetCommandSetting(conf.NameStreamRestoreCmd)
			tracelog.ErrorLogger.FatalOnError(err)
			prepareCmd, _ := internal.GetCommandSetting(conf.MysqlBackupPrepareCmd)

			targetBackupSelector, err := createTargetBackupSelector(args, fetchTargetUserData)
			tracelog.ErrorLogger.FatalOnError(err)

			mysql.HandleBackupFetch(storage.RootFolder(), targetBackupSelector, restoreCmd, prepareCmd)
		},
	}
	fetchTargetUserData string
)

func createTargetBackupSelector(args []string, fetchTargetUserData string) (internal.BackupSelector, error) {
	if fetchTargetUserData == "" {
		fetchTargetUserData = viper.GetString(conf.FetchTargetUserDataSetting)
	}
	fetchTargetBackupName := ""
	if len(args) >= 1 {
		fetchTargetBackupName = args[0]
	}
	return internal.NewTargetBackupSelector(fetchTargetUserData, fetchTargetBackupName, mysql.NewGenericMetaFetcher())
}

func init() {
	cmd.AddCommand(backupFetchCmd)
	backupFetchCmd.Flags().StringVar(&fetchTargetUserData, "target-user-data",
		"", targetUserDataDescription)
}
