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
	backupFetchShortDescription         = "Fetch desired backup from storage"
	targetUserDataDescription           = "Fetch storage backup which has the specified user data"
	inplaceDiffBackupRestoreDescription = "Restore diff-backups in place (without extra space)"
)

var (
	// backupFetchCmd represents the streamFetch command
	backupFetchCmd = &cobra.Command{
		Use:   "backup-fetch backup-name",
		Short: backupFetchShortDescription,
		Args:  cobra.RangeArgs(0, 1),
		Run: func(cmd *cobra.Command, args []string) {
			internal.ConfigureLimiters()
			storage, err := internal.ConfigureStorage()
			tracelog.ErrorLogger.FatalOnError(err)
			restoreCmd, err := internal.GetCommandSetting(conf.NameStreamRestoreCmd)
			if !inplaceDiffBackupRestore {
				tracelog.ErrorLogger.FatalOnError(err)
			}
			prepareCmd, _ := internal.GetCommandSetting(conf.MysqlBackupPrepareCmd)

			targetBackupSelector, err := createTargetBackupSelector(args, fetchTargetUserData)
			tracelog.ErrorLogger.FatalOnError(err)

			mysql.HandleBackupFetch(storage.RootFolder(), targetBackupSelector, restoreCmd, prepareCmd, inplaceDiffBackupRestore)
		},
	}
	fetchTargetUserData      string
	inplaceDiffBackupRestore bool
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
	backupFetchCmd.Flags().BoolVar(&inplaceDiffBackupRestore, "inplace-diff-backup-restore",
		false, inplaceDiffBackupRestoreDescription)
	_ = backupFetchCmd.Flags().MarkHidden("inplace-diff-backup-restore")
}
