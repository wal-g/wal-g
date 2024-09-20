package redis

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/utility"
)

var tag string

var backupInfoCmd = &cobra.Command{
	Use:   "backup-info",
	Short: "Prints redis backup info",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		storage, err := internal.ConfigureStorage()
		tracelog.ErrorLogger.FatalOnError(err)

		backupName := args[0]
		redis.HandleBackupInfo(storage.RootFolder(), backupName, os.Stdout, tag)
	},
}

func init() {
	backupInfoCmd.PersistentFlags().StringVar(&tag, "tag", "", "print specified field value only")
	cmd.AddCommand(backupInfoCmd)
}
