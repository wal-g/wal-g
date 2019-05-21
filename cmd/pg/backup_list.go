package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/tracelog"
)

const (
	BackupListShortDescription = "Prints available backups"
	PrettyFlag                 = "pretty"
	JsonFlag                   = "json"
)

var (
	// backupListCmd represents the backupList command
	backupListCmd = &cobra.Command{
		Use:   "backup-list",
		Short: BackupListShortDescription, // TODO : improve description
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			folder, err := internal.ConfigureFolder()
			if err != nil {
				tracelog.ErrorLogger.FatalError(err)
			}
			if pretty || json {
				internal.HandleBackupListWithFlags(folder, pretty, json)
			} else {
				internal.HandleBackupList(folder)
			}
		},
	}
	pretty = false
	json   = false
)

func init() {
	PgCmd.AddCommand(backupListCmd)

	backupListCmd.Flags().BoolVar(&pretty, PrettyFlag, false, "Prints more readable output")
	backupListCmd.Flags().BoolVar(&json, JsonFlag, false, "Prints output in json format")
}
