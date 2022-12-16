package mysql

import (
	"github.com/wal-g/wal-g/internal/databases/mysql"

	"github.com/spf13/cobra"
)

const (
	binlogServerShortDescription = "Create server for backup slaves"
)

var (
	binlogServerCmd = &cobra.Command{
		Use:   "binlog-server",
		Short: binlogServerShortDescription,
		Args:  cobra.NoArgs,
		Run: func(cmd *cobra.Command, args []string) {
			mysql.HandleBinlogServer()
		},
	}
)

func init() {
	cmd.AddCommand(binlogServerCmd)
}
