package fdb

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/fdb"
	"github.com/wal-g/wal-g/utility"
)

const backupFetchShortDescription = "Fetches desired backup from storage"

// backupFetchCmd represents the streamFetch command
var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch backup-name",
	Short: backupFetchShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		storage, err := internal.ConfigureStorage()
		tracelog.ErrorLogger.FatalOnError(err)

		restoreCmd, err := internal.GetCommandSettingContext(ctx, conf.NameStreamRestoreCmd)
		tracelog.ErrorLogger.FatalOnError(err)
		targetBackupSelector, err := internal.NewBackupNameSelector(args[0], true)
		tracelog.ErrorLogger.FatalOnError(err)
		fdb.HandleBackupFetch(ctx, storage.RootFolder(), targetBackupSelector, restoreCmd)
	},
}

func init() {
	cmd.AddCommand(backupFetchCmd)
}
