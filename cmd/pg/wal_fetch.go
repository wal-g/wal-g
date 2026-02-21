package pg

import (
	"log/slog"
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/databases/postgres/constants"
	"github.com/wal-g/wal-g/internal/logging"
)

const WalFetchShortDescription = "Fetches a WAL file from storage"

// walFetchCmd represents the walFetch command
var walFetchCmd = &cobra.Command{
	Use:   "wal-fetch wal_name destination_filename",
	Short: WalFetchShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		storage, err := internal.ConfigureMultiStorage(false)
		tracelog.ErrorLogger.FatalfOnError("Failed to configure multi-storage: %v", err)

		folderReader, err := internal.PrepareMultiStorageFolderReader(storage.RootFolder(), targetStorage)
		logging.FatalOnError(err)

		err = postgres.HandleWALFetch(folderReader, args[0], args[1], postgres.RegularPrefetcher{})
		if _, isArchNonExistErr := err.(internal.ArchiveNonExistenceError); isArchNonExistErr {
			slog.Error(err.Error())
			os.Exit(constants.ExIoError)
		}
		logging.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(walFetchCmd)
	walFetchCmd.Flags().StringVar(&targetStorage, "target-storage", "", targetStorageDescription)
}
