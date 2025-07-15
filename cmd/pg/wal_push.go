package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const WalPushShortDescription = "Uploads a WAL file to storage"

// walPushCmd represents the walPush command
var walPushCmd = &cobra.Command{
	Use:   "wal-push wal_filepath",
	Short: WalPushShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		storage, err := internal.ConfigureMultiStorage(true)
		tracelog.ErrorLogger.FatalfOnError("Failed to configure multi-storage: %v", err)

		walUploader, err := postgres.PrepareMultiStorageWalUploader(storage.RootFolder(), targetStorage)
		tracelog.ErrorLogger.FatalOnError(err)

		err = postgres.HandleWALPush(cmd.Context(), walUploader, args[0])
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(walPushCmd)
	walPushCmd.Flags().StringVar(&targetStorage, "target-storage", "", targetStorageDescription)
}
