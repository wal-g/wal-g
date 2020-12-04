package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const (
	BackupListShortDescription = "Prints available backups"
	PrettyFlag                 = "pretty"
	JsonFlag                   = "json"
	DetailFlag                 = "detail"
)

var (
	// backupListCmd represents the backupList command
	backupListCmd = &cobra.Command{
		Use:   "backup-list",
		Short: BackupListShortDescription, // TODO : improve description
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			folder, err := internal.ConfigureFolder()
			tracelog.ErrorLogger.FatalOnError(err)
			if pretty || json || detail {
				internal.HandleBackupListWithFlags(folder, pretty, json, detail)
			} else {
				internal.DefaultHandleBackupList(folder)
			}
		},
	}
	pretty = false
	json   = false
	detail = false
)

func init() {
	cmd.AddCommand(backupListCmd)

	backupListCmd.Flags().BoolVar(&pretty, PrettyFlag, false, "Prints more readable output")
	backupListCmd.Flags().BoolVar(&json, JsonFlag, false, "Prints output in json format")
	backupListCmd.Flags().BoolVar(&detail, DetailFlag, false, "Prints extra backup details")
}
