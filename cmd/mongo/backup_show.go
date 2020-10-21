package mongo

import (
	"context"
	"encoding/json"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/mongo"
	"github.com/wal-g/wal-g/internal/databases/mongo/archive"
	"github.com/wal-g/wal-g/internal/databases/mongo/models"
	"github.com/wal-g/wal-g/utility"
)

const BackupShowShortDescription = "Prints information about backup"

// backupShowCmd represents the backupList command
var backupShowCmd = &cobra.Command{
	Use:   "backup-show <backup-name>",
	Short: BackupShowShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		// set up storage downloader client
		downloader, err := archive.NewStorageDownloader(archive.NewDefaultStorageSettings())
		tracelog.ErrorLogger.FatalOnError(err)

		err = mongo.HandleBackupShow(
			downloader,
			args[0],
			func(b models.Backup) (bytes []byte, err error) {
				return json.Marshal(b)
			},
			os.Stdout)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(backupShowCmd)
}
