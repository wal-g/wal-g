package st

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/internal/multistorage/exec"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

const (
	getStreamShortDescription = "Download the backup as single stream"
	getStreamLongDescription  = "Download, decrypt (if encrypted), decompress (if compressed) and assemble (if stream where split) single-stream backups to single stream."
)

// getObjectCmd represents the getObject command
var getStreamCmd = &cobra.Command{
	Use:   "get backup_name destination_path",
	Short: getStreamShortDescription,
	Long:  getStreamLongDescription,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		backupName := args[0]
		dstPath := args[1]

		if targetStorage == "all" {
			tracelog.ErrorLogger.Fatalf("'all' target is not supported for st get command")
		}

		backupSelector, err := internal.NewTargetBackupSelector("", backupName, mysql.NewGenericMetaFetcher())
		tracelog.ErrorLogger.FatalOnError(err)

		file, err := os.Create(dstPath)
		defer utility.LoggedClose(file, "got an error during stream-file close()")

		err = exec.OnStorage(targetStorage, func(folder storage.Folder) error {
			backup, err := backupSelector.Select(folder)
			tracelog.ErrorLogger.FatalOnError(err)
			fetcher, err := internal.GetBackupStreamFetcher(backup)
			tracelog.ErrorLogger.FatalfOnError("Failed to detect backup format: %v\n", err)

			return fetcher(backup, file)
		})
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	StorageToolsCmd.AddCommand(getStreamCmd)
}
