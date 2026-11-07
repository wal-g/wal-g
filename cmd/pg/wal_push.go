package pg

import (
	"context"
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
)

const WalPushShortDescription = "Uploads a WAL file to storage"

var timeout time.Duration

// walPushCmd represents the walPush command
var walPushCmd = &cobra.Command{
	Use:   "wal-push wal_filepath",
	Short: WalPushShortDescription, // TODO : improve description
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		if timeout > 0 {
			var cancel context.CancelFunc
			ctx, cancel = context.WithTimeout(ctx, timeout)
			defer cancel()
		}

		storage, err := internal.ConfigureMultiStorage(ctx, true)
		tracelog.ErrorLogger.FatalfOnError("Failed to configure multi-storage: %v", err)

		walUploader, err := postgres.PrepareMultiStorageWalUploader(ctx, storage.RootFolder(), targetStorage)
		tracelog.ErrorLogger.FatalOnError(err)

		err = postgres.HandleWALPush(ctx, walUploader, args[0])
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(walPushCmd)
	walPushCmd.Flags().StringVar(&targetStorage, "target-storage", "", targetStorageDescription)
	walPushCmd.Flags().DurationVar(&timeout, "timeout", 0, "Overall operation timeout (e.g. 1s, 2m)")
}
