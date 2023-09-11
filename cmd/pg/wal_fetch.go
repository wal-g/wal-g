package pg

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/databases/postgres/constants"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/cache"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

const WalFetchShortDescription = "Fetches a WAL file from storage"

// walFetchCmd represents the walFetch command
var walFetchCmd = &cobra.Command{
	Use:   "wal-fetch wal_name destination_filename",
	Short: WalFetchShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		folderReader := GetWalFolderReader()

		err := postgres.HandleWALFetch(folderReader, args[0], args[1], postgres.RegularPrefetcher{})
		if _, isArchNonExistErr := err.(internal.ArchiveNonExistenceError); isArchNonExistErr {
			tracelog.ErrorLogger.Print(err.Error())
			os.Exit(constants.ExIoError)
		}
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func GetWalFolderReader() internal.StorageFolderReader {
	folder := GetFolder()
	folder = multistorage.SetPolicies(folder, policies.MergeAllStorages)
	folder, err := multistorage.UseAllAliveStorages(folder)
	tracelog.ErrorLogger.FatalOnError(err)

	return internal.NewFolderReader(folder)
}

func GetFolder() storage.Folder {
	primaryStorage, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	failoverStorages, err := internal.InitFailoverStorages()
	tracelog.ErrorLogger.FatalOnError(err)

	cacheLifetime, err := internal.GetDurationSetting(internal.PgFailoverStorageCacheLifetime)
	tracelog.ErrorLogger.FatalOnError(err)
	aliveCheckTimeout, err := internal.GetDurationSetting(internal.PgFailoverStoragesCheckTimeout)
	tracelog.ErrorLogger.FatalOnError(err)
	aliveCheckSize := viper.GetSizeInBytes(internal.PgFailoverStoragesCheckSize)
	statusCache, err := cache.NewStatusCache(
		primaryStorage,
		failoverStorages,
		cacheLifetime,
		aliveCheckTimeout,
		aliveCheckSize,
		false,
	)
	tracelog.ErrorLogger.FatalOnError(err)

	return multistorage.NewFolder(statusCache)
}

func init() {
	Cmd.AddCommand(walFetchCmd)
}
