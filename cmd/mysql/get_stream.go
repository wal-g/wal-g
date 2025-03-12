package mysql

import (
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
	"github.com/wal-g/wal-g/internal/multistorage/consts"
	"github.com/wal-g/wal-g/internal/multistorage/exec"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

var targetStorage string

const (
	getStreamShortDescription = "Download the backup as single stream"
	getStreamLongDescription  = "Download, decrypt (if encrypted), decompress (if compressed) and assemble " +
		"(if stream where split) single-stream backups to single stream."
)

// getObjectCmd represents the getObject command
var getStreamCmd = &cobra.Command{
	Use:   "get-stream backup_name destination_path",
	Short: getStreamShortDescription,
	Long:  getStreamLongDescription,
	Args:  cobra.RangeArgs(1, 2),
	Run: func(cmd *cobra.Command, args []string) {
		backupName := args[0]
		outStream := os.Stdout

		if len(args) == 2 {
			dstPath := args[1]
			file, err := os.Create(dstPath)
			tracelog.ErrorLogger.FatalOnError(err)
			defer utility.LoggedClose(file, "got an error during stream-file close()")
			outStream = file
		}

		if targetStorage == "all" {
			tracelog.ErrorLogger.Fatalf("'all' target is not supported for st get command")
		}

		backupSelector, err := internal.NewTargetBackupSelector("", backupName, mysql.NewGenericMetaFetcher())
		tracelog.ErrorLogger.FatalOnError(err)

		err = exec.OnStorage(targetStorage, func(folder storage.Folder) error {
			backup, err := backupSelector.Select(folder)
			tracelog.ErrorLogger.FatalOnError(err)
			fetcher, err := internal.GetBackupStreamFetcher(backup)
			tracelog.ErrorLogger.FatalfOnError("Failed to detect backup format: %v\n", err)

			return fetcher(backup, outStream)
		})
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	cmd.PersistentFlags().StringVarP(&targetStorage, "target", "", consts.DefaultStorage,
		"execute for specific failover storage (Postgres only)")

	cmd.AddCommand(getStreamCmd)
}
