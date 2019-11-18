package pg

import (
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"

	"github.com/spf13/cobra"
)

const (
	BackupPushShortDescription = "Makes backup and uploads it to storage"
	PermanentFlag              = "permanent"
	FullBackupFlag             = "full"
	PermanentShorthand         = "p"
	FullBackupShorthand        = "f"
)

var (
	// backupPushCmd represents the backupPush command
	backupPushCmd = &cobra.Command{
		Use:   "backup-push db_directory",
		Short: BackupPushShortDescription, // TODO : improve description
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			uploader, err := internal.ConfigureUploader(true)
			tracelog.ErrorLogger.FatalOnError(err)
			internal.HandleBackupPush(uploader, args[0], permanent, fullBackup)
		},
	}
	permanent  = false
	fullBackup = false
)

func init() {
	Cmd.AddCommand(backupPushCmd)

	backupPushCmd.Flags().BoolVarP(&permanent, PermanentFlag, PermanentShorthand, false, "Pushes permanent backup")
	backupPushCmd.Flags().BoolVarP(&fullBackup, FullBackupFlag, FullBackupShorthand, false, "Make full backup-push")
}
