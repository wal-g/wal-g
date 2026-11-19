package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/statistics"
)

const WalPushShortDescription = "Uploads a WAL file to storage"

// walPushCmd represents the walPush command
var walPushCmd = &cobra.Command{
	Use:   "wal-push wal_filepath",
	Short: WalPushShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// Use statistics.Fatal* so metrics are pushed even when wal-push fails.
		storage, err := internal.ConfigureMultiStorage(cmd.Context(), true)
		statistics.FatalfOnError("Failed to configure multi-storage: %v", err)

		walUploader, err := postgres.PrepareMultiStorageWalUploader(cmd.Context(), storage.RootFolder(), targetStorage)
		statistics.FatalOnError(err)

		err = postgres.HandleWALPush(cmd.Context(), walUploader, args[0])
		statistics.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(walPushCmd)
	walPushCmd.Flags().StringVar(&targetStorage, "target-storage", "", targetStorageDescription)
}
