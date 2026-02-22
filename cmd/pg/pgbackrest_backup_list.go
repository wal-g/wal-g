package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/databases/postgres/pgbackrest"
	"github.com/wal-g/wal-g/internal/logging"
)

var pgbackrestBackupListCmd = &cobra.Command{
	Use:   "backup-list",
	Short: backupListShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		folder, stanza := configurePgbackrestSettings()
		err := pgbackrest.HandleBackupList(folder, stanza, detail, pretty, json)
		logging.FatalOnError(err)
	},
}

func init() {
	pgbackrestCmd.AddCommand(pgbackrestBackupListCmd)

	pgbackrestBackupListCmd.Flags().BoolVar(&pretty, PrettyFlag, false, "Prints more readable output")
	pgbackrestBackupListCmd.Flags().BoolVar(&json, JSONFlag, false, "Prints output in json format")
	pgbackrestBackupListCmd.Flags().BoolVar(&detail, DetailFlag, false, "Prints extra backup details")
}
