package mongo

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/utility"
)

const binaryBackupPushCommandName = "binary-backup-push"

var binaryBackupPushCmd = &cobra.Command{
	Use:   binaryBackupPushCommandName,
	Short: "Creates mongodb binary backup and pushes it to storage without local disk",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		err := mongo.HandleBinaryBackupPush(ctx, permanent, "wal-g-mongo "+binaryBackupPushCommandName)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	binaryBackupPushCmd.Flags().BoolVarP(&permanent, PermanentFlag, PermanentShorthand, false, "Pushes permanent backup")
	cmd.AddCommand(binaryBackupPushCmd)
}
