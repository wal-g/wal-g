package redis

import (
	"context"
	"os"
	"syscall"

	"github.com/wal-g/wal-g/internal/databases/redis"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/utility"
)

const backupDeleteShortDescription = "Deletes backup data from storage"

var (
	confirmedBackupDelete bool
)

// backupDeleteCmd represents the backupDelete command
var backupDeleteCmd = &cobra.Command{
	Use:   "backup-delete <backup-name>",
	Short: backupDeleteShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		backupName := args[0]

		storage, err := internal.ConfigureStorage()
		tracelog.ErrorLogger.FatalOnError(err)
		err = redis.HandleBackupDelete(storage.RootFolder(), backupName, !confirmedBackupDelete)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	backupDeleteCmd.Flags().BoolVar(&confirmedBackupDelete, internal.ConfirmFlag, false, "Confirms backup deletion")
	cmd.AddCommand(backupDeleteCmd)
}
