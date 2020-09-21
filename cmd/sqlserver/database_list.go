package sqlserver

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/databases/sqlserver"
)

const databaseListShortDescription = "List datbases in the backup"

var databaseListCmd = &cobra.Command{
	Use:   "database-list",
	Short: databaseListShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		sqlserver.HandleDatabaseList(args[0])
	},
}

func init() {
	Cmd.AddCommand(databaseListCmd)
}
