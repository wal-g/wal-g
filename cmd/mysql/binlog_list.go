package mysql

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mysql"
)

const binlogListShortDescription = "List available binlogs in storage"

var listSince string
var listUntil string
var prettyOutput bool
var jsonOutput bool

var binlogListCmd = &cobra.Command{
	Use:   "binlog-list",
	Short: binlogListShortDescription,
	Args:  cobra.NoArgs,
	PreRun: func(cmd *cobra.Command, args []string) {
		err := internal.AssertRequiredSettingsSet()
		tracelog.ErrorLogger.FatalOnError(err)
	},
	Run: func(cmd *cobra.Command, args []string) {
		storage, err := internal.ConfigureStorage()
		tracelog.ErrorLogger.FatalOnError(err)
		mysql.HandleBinlogList(storage.RootFolder(), listSince, listUntil, prettyOutput, jsonOutput)
	},
}

func init() {
	binlogListCmd.PersistentFlags().StringVar(&listSince, "since", "", "show binlogs modified since this time (e.g., '2h', '30m', '2023-01-01T10:00:00Z', 'LATEST')")
	binlogListCmd.PersistentFlags().StringVar(&listUntil, "until", "", "show binlogs modified until this time (e.g., '1h', '2023-01-01T12:00:00Z')")
	binlogListCmd.PersistentFlags().BoolVar(&prettyOutput, "pretty", false, "pretty print the output (table format)")
	binlogListCmd.PersistentFlags().BoolVar(&jsonOutput, "json", false, "output in JSON format")
	cmd.AddCommand(binlogListCmd)
}
