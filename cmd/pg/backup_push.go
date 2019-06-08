package pg

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"

	"github.com/spf13/cobra"
)

const (
	BackupPushShortDescription = "Makes backup and uploads it to storage"
	PermanentFlag              = "permanent"
)

// backupPushCmd represents the backupPush command
var (
	backupPushCmd = &cobra.Command{
		Use:   "backup-push db_directory",
		Short: BackupPushShortDescription, // TODO : improve description
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			uploader, err := internal.ConfigureUploader()
			if err != nil {
				tracelog.ErrorLogger.FatalError(err)
			}
			internal.HandleBackupPush(uploader, args[0], permanent)
		},
	}
	permanent = false
)

func init() {
	PgCmd.AddCommand(backupPushCmd)

	backupPushCmd.Flags().BoolVar(&permanent, PermanentFlag, false, "Pushes permanent backup")
}
