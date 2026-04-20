package mysql

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/internal/logging"
)

const (
	backupFetchShortDescription = "Fetch desired backup from storage"
	targetUserDataDescription   = "Fetch storage backup which has the specified user data"
	useXbtoolExtractDescription = "Use internal xbtool to extract data from xbstream"
	inplaceDescription          = "(DANGEROUS) Apply diff-s inplace (reduce required disk space)"
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
			logging.FatalOnError(err)
			restoreCmd, err := internal.GetCommandSetting(conf.NameStreamRestoreCmd)
			if !useXbtoolExtract {
				logging.FatalOnError(err)
			}
			prepareCmd, _ := internal.GetCommandSetting(conf.MysqlBackupPrepareCmd)

			targetBackupSelector, err := createTargetBackupSelector(args, fetchTargetUserData)
			logging.FatalOnError(err)

			mysql.HandleBackupFetch(storage.RootFolder(), targetBackupSelector, restoreCmd, prepareCmd, useXbtoolExtract, inplace)
		},
	}
	fetchTargetUserData string
	useXbtoolExtract    bool
	inplace             bool
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
	backupFetchCmd.Flags().StringVar(&fetchTargetUserData, "target-user-data", "", targetUserDataDescription)
	backupFetchCmd.Flags().BoolVar(&useXbtoolExtract, "use-xbtool-extract", false, useXbtoolExtractDescription)
	backupFetchCmd.Flags().BoolVar(&inplace, "inplace", false, inplaceDescription)
	_ = backupFetchCmd.Flags().MarkHidden("use-xbtool-extract")
	_ = backupFetchCmd.Flags().MarkHidden("inplace")
}
