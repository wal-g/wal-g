package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
)

const (
	BackupMarkShortDescription = "change atrribute on backup"
	BackupMarkLongDescription  = `man chattr; but only i flag for now`
	ImmutableDescription       = "Marks a backup immutable"
	ImmutableFlag              = "immutable"
)


func HandleBackupChattr(uploader *internal.Uploader, folder storage.Folder, backupName string) {
	mysql.ChattrBackup(uploader, folder, backupName, []string{}, []string{})
}

var (

	chattrI = false
	// backupChattrCmd represents the backupMark command
	backupChattrCmd = &cobra.Command{
		Use:   "backup-chattr",
		Short: BackupMarkShortDescription,
		Long:  BackupMarkLongDescription,
		Args:  cobra.ExactArgs(1),
		Run: func(cmd *cobra.Command, args []string) {
			uploader, err := internal.ConfigureWalUploader()
			tracelog.ErrorLogger.FatalOnError(err)

			HandleBackupChattr(uploader.Uploader,
				uploader.UploadingFolder, args[0])
		},
	}
)

func init() {
	backupChattrCmd.Flags().BoolVarP(&chattrI, ImmutableFlag, "i", false, ImmutableDescription)
	Cmd.AddCommand(backupChattrCmd)
}

