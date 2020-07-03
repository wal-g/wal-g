package mongo

import (
	"context"
	"os"
	"syscall"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/utility"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
)

const BackupFetchShortDescription = "Fetches desired backup from storage"

// backupFetchCmd represents the streamFetch command
var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch backup-name",
	Short: BackupFetchShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		restoreCmd, err := internal.GetCommandSettingContext(ctx, internal.NameStreamRestoreCmd)
		tracelog.ErrorLogger.FatalOnError(err)
		restoreCmd.Stdout = os.Stdout
		restoreCmd.Stderr = os.Stderr

		err = mongo.HandleBackupFetch(ctx, folder, args[0], restoreCmd)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(backupFetchCmd)
}
