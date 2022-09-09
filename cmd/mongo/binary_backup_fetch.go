package mongo

import (
	"context"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/utility"
)

const binaryBackupFetchCommandName = "binary-backup-fetch"

var binaryBackupFetchCmd = &cobra.Command{
	Use:   binaryBackupFetchCommandName + " <backup name> <mongod config path> <mongod version>",
	Short: "Fetches a mongodb binary backup from storage and restores it in mongodb storage dbPath",
	Args:  cobra.ExactArgs(3),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		backupName := args[0]
		mongodbConfigPath := args[1]
		mongodVersion := args[2]

		err := mongo.HandleBinaryFetchPush(ctx, mongodbConfigPath, backupName, mongodVersion)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	cmd.AddCommand(binaryBackupFetchCmd)
}
