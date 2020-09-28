package sqlserver

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal/databases/sqlserver"
)

const logPushShortDescription = "Creates new log backup and pushes it to the storage"

var logPushDatabases []string
var logCompression bool

var logPushCmd = &cobra.Command{
	Use:   "log-push",
	Short: logPushShortDescription,
	Run: func(cmd *cobra.Command, args []string) {
		sqlserver.HandleLogPush(logPushDatabases, logCompression)
	},
}

func init() {
	logPushCmd.PersistentFlags().StringSliceVarP(&logPushDatabases, "databases", "d", []string{},
		"List of databases to log. All not-system databases as default")
	logPushCmd.PersistentFlags().BoolVarP(&logCompression, "compression", "c", true,
		"Use built-in log compression. Enabled by default")
	Cmd.AddCommand(logPushCmd)
}
