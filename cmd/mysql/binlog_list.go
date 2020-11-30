package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

const binlogsListShortDescription = "Prints available binlogs"

// backupListCmd represents the backupList command
var binlogListCmd = &cobra.Command{
	Use:   "binlog-list",
	Short: binlogsListShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		tracelog.ErrorLogger.FatalOnError(HandleBinlogList(folder))
	},
}

func init() {
	Cmd.AddCommand(binlogListCmd)
}
