package pg

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/multistorage"
)

const WalFetchShortDescription = "Fetches a WAL file from storage"

// walFetchCmd represents the walFetch command
var walFetchCmd = &cobra.Command{
	Use:   "wal-fetch wal_name destination_filename",
	Short: WalFetchShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		folderReader := GetFolderReader()
		err := postgres.HandleWALFetch(folderReader, args[0], args[1], postgres.RegularPrefetcher{})
		if _, isArchNonExistErr := err.(internal.ArchiveNonExistenceError); isArchNonExistErr {
			tracelog.ErrorLogger.Print(err.Error())
			os.Exit(postgres.ExIoError)
		}
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func GetFolderReader() internal.StorageFolderReader {
	folder, err := internal.ConfigureFolder()
	tracelog.ErrorLogger.FatalOnError(err)

	failover, err := internal.InitFailoverStorages()
	tracelog.ErrorLogger.FatalOnError(err)

	folderReader, err := multistorage.NewStorageFolderReader(folder, failover)
	tracelog.ErrorLogger.FatalOnError(err)

	return folderReader
}

func init() {
	Cmd.AddCommand(walFetchCmd)
}
