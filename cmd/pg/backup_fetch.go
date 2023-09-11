package pg

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/cache"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
)

const (
	backupFetchShortDescription = "Fetches a backup from storage"
	maskFlagDescription         = `Fetches only files which path relative to destination_directory
matches given shell file pattern.
For information about pattern syntax view: https://golang.org/pkg/path/filepath/#Match`
	restoreSpecDescription        = "Path to file containing tablespace restore specification"
	reverseDeltaUnpackDescription = "Unpack delta backups in reverse order (beta feature)"
	skipRedundantTarsDescription  = "Skip tars with no useful data (requires reverse delta unpack)"
	targetUserDataDescription     = "Fetch storage backup which has the specified user data"
	restoreOnlyDescription        = `[Experimental] Downloads only databases or tables specified by passed names.
Separate parameters with comma. Use 'database' or 'database/namespace.table' as a parameter ('public' namespace can be omitted).  
Sets reverse delta unpack & skip redundant tars options automatically. Always downloads system databases and tables.`
)

var fileMask string
var restoreSpec string
var reverseDeltaUnpack bool
var skipRedundantTars bool
var fetchTargetUserData string
var partialRestoreArgs []string

var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch destination_directory [backup_name | --target-user-data <data>]",
	Short: backupFetchShortDescription, // TODO : improve description
	Args:  cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		if fetchTargetUserData == "" {
			fetchTargetUserData = viper.GetString(internal.FetchTargetUserDataSetting)
		}
		targetBackupSelector, err := createTargetFetchBackupSelector(cmd, args, fetchTargetUserData)
		tracelog.ErrorLogger.FatalOnError(err)

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

		if partialRestoreArgs != nil {
			skipRedundantTars = true
			reverseDeltaUnpack = true
		}
		reverseDeltaUnpack = reverseDeltaUnpack || viper.GetBool(internal.UseReverseUnpackSetting)
		skipRedundantTars = skipRedundantTars || viper.GetBool(internal.SkipRedundantTarsSetting)

		var extractProv postgres.ExtractProvider

		if partialRestoreArgs != nil {
			extractProv = postgres.NewExtractProviderDBSpec(partialRestoreArgs)
		} else {
			extractProv = postgres.ExtractProviderImpl{}
		}

		var pgFetcher internal.Fetcher
		if reverseDeltaUnpack {
			pgFetcher = postgres.GetFetcherNew(args[0], fileMask, restoreSpec, skipRedundantTars, extractProv)
		} else {
			pgFetcher = postgres.GetFetcherOld(args[0], fileMask, restoreSpec, extractProv)
		}

		internal.HandleBackupFetch(folder, targetBackupSelector, pgFetcher)
	},
}

// create the BackupSelector to select the backup to fetch
func createTargetFetchBackupSelector(cmd *cobra.Command,
	args []string, targetUserData string) (internal.BackupSelector, error) {
	targetName := ""
	if len(args) >= 2 {
		targetName = args[1]
	}

	backupSelector, err := internal.NewTargetBackupSelector(targetUserData, targetName, postgres.NewGenericMetaFetcher())
	if err != nil {
		fmt.Println(cmd.UsageString())
		return nil, err
	}
	return backupSelector, nil
}

func init() {
	backupFetchCmd.Flags().StringVar(&fileMask, "mask", "", maskFlagDescription)
	backupFetchCmd.Flags().StringVar(&restoreSpec, "restore-spec", "", restoreSpecDescription)
	backupFetchCmd.Flags().BoolVar(&reverseDeltaUnpack, "reverse-unpack",
		false, reverseDeltaUnpackDescription)
	backupFetchCmd.Flags().BoolVar(&skipRedundantTars, "skip-redundant-tars",
		false, skipRedundantTarsDescription)
	backupFetchCmd.Flags().StringVar(&fetchTargetUserData, "target-user-data",
		"", targetUserDataDescription)
	backupFetchCmd.Flags().StringSliceVar(&partialRestoreArgs, "restore-only",
		nil, restoreOnlyDescription)

	Cmd.AddCommand(backupFetchCmd)
}
