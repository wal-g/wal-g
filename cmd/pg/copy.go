package pg

import (
	"fmt"

	"github.com/spf13/cobra"
)

const (
	backupCopyUsage            = "copy backup_name"
	backupCopyShortDescription = "Copy backup to another storage"
	backupCopyLongDescription  = "Copy backup with specific name from one storage to another according to configs with history(by default)"

	fromFlag        = "from"
	fromShorthand   = "f"
	fromDescription = "Storage config from where should copy backup"

	toFlag        = "to"
	toShorthand   = "t"
	toDescription = "Storage config to where should copy backup"

	withoutHistoryFlag        = "without-history"
	withoutHistoryShorthand   = "w"
	withoutHistoryDescription = "Copy backup without history"
)

var (
	from           = ""
	to             = ""
	withoutHistory = false

	backupCopyCmd = &cobra.Command{
		Use:   backupCopyUsage,
		Short: backupCopyShortDescription,
		Long:  backupCopyLongDescription,
		Args:  cobra.ExactArgs(1),
		Run:   runBackupCopy,
	}
)

func runBackupCopy(cmd *cobra.Command, args []string) {
	fmt.Print("Not implemented")
}

func init() {
	Cmd.AddCommand(backupCopyCmd)

	backupCopyCmd.Flags().StringVarP(&from, fromFlag, fromShorthand, "", fromDescription)
	backupCopyCmd.Flags().StringVarP(&to, toFlag, toShorthand, "", toDescription)
	backupCopyCmd.Flags().BoolVarP(&withoutHistory, withoutHistoryFlag, withoutHistoryShorthand, false, withoutHistoryDescription)

	backupCopyCmd.MarkFlagFilename(from)
	backupCopyCmd.MarkFlagFilename(to)
	backupCopyCmd.MarkFlagRequired(from)
	backupCopyCmd.MarkFlagRequired(to)
}
