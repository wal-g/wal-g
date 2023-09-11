package pg

import (
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/cache"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/utility"
)

const (
	backupListShortDescription = "Prints full list of backups from which recovery is available"
	PrettyFlag                 = "pretty"
	JSONFlag                   = "json"
	DetailFlag                 = "detail"
)

var (
	// backupListCmd represents the backupList command
	backupListCmd = &cobra.Command{
		Use:   "backup-list",
		Short: backupListShortDescription,
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, _ []string) {
			primaryStorage, err := internal.ConfigureFolder()
			tracelog.ErrorLogger.FatalOnError(err)

			failoverStorages, err := internal.InitFailoverStorages()
			tracelog.ErrorLogger.FatalOnError(err)

			cacheLifetime, err := internal.GetDurationSetting(internal.PgFailoverStorageCacheLifetime)
			tracelog.ErrorLogger.FatalOnError(err)
			aliveCheckTimeout, err := internal.GetDurationSetting(internal.PgFailoverStoragesCheckTimeout)
			tracelog.ErrorLogger.FatalOnError(err)
			aliveCheckSize := viper.GetSizeInBytes(internal.PgFailoverStoragesCheckSize)
			cache, err := cache.NewStatusCache(
				primaryStorage,
				failoverStorages,
				cacheLifetime,
				aliveCheckTimeout,
				aliveCheckSize,
				false,
			)
			tracelog.ErrorLogger.FatalOnError(err)

			folder := multistorage.NewFolder(cache)
			folder = multistorage.SetPolicies(folder, policies.UniteAllStorages)
			folder, err = multistorage.UseAllAliveStorages(folder)
			tracelog.ErrorLogger.FatalOnError(err)

			backupsFolder := folder.GetSubFolder(utility.BaseBackupPath)
			if detail {
				postgres.HandleDetailedBackupList(backupsFolder, pretty, json)
			} else {
				internal.HandleDefaultBackupList(backupsFolder, pretty, json)
			}
		},
	}
	pretty = false
	json   = false
	detail = false
)

func init() {
	Cmd.AddCommand(backupListCmd)

	// TODO: Merge similar backup-list functionality
	// to avoid code duplication in command handlers
	backupListCmd.Flags().BoolVar(&pretty, PrettyFlag, false,
		"Prints more readable output in table format")
	backupListCmd.Flags().BoolVar(&json, JSONFlag, false,
		"Prints output in JSON format, multiline and indented if combined with --pretty flag")
	backupListCmd.Flags().BoolVar(&detail, DetailFlag, false,
		"Prints extra DB-specific backup details")
}
