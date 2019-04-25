package all

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/cmd/mysql"
)

const MySQLShortDescription = "Set of commands for MySQL"

var mysqlCmd = &cobra.Command{
	Use:     "mysql",
	Short:   MySQLShortDescription, // TODO : improve short and long descriptions
}

func init() {
	mysqlCmd.AddCommand(mysql.BackupListCmd)
	mysqlCmd.AddCommand(mysql.BinlogPushCmd)
	mysqlCmd.AddCommand(mysql.DeleteCmd)
	mysqlCmd.AddCommand(mysql.StreamFetchCmd)
	mysqlCmd.AddCommand(mysql.StreamPushCmd)

	RootCmd.AddCommand(mysqlCmd)
}
