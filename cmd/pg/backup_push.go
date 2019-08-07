package pg

import (
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"

	"github.com/spf13/cobra"
)

const (
	BackupPushShortDescription = "Makes backup and uploads it to storage"
	PermanentFlag              = "permanent"
	FullBackupFlag			   = "full"
	PermanentShorthand		   = "p"
	FullBackupShorthand		   = "f"
)

var (
	// backupPushCmd represents the backupPush command
	backupPushCmd = &cobra.Command{
		Use:   "backup-push db_directory",
		Short: BackupPushShortDescription, // TODO : improve description
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			uploader, err := internal.ConfigureUploader()
			if err != nil {
				tracelog.ErrorLogger.FatalError(err)
			}
			internal.HandleBackupPush(uploader, args[0], permanent, full_backup)
		},
	}
	permanent = false
	full_backup = false
)

func init() {
	PgCmd.AddCommand(backupPushCmd)

	backupPushCmd.Flags().BoolVarP(&permanent, PermanentFlag, PermanentShorthand, false, "Pushes permanent backup")
	backupPushCmd.Flags().BoolVarP(&full_backup, FullBackupFlag, FullBackupShorthand, false, "Make full backup-push")
}
