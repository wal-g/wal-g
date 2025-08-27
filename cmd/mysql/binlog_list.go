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

var binlogListCmd = &cobra.Command{
	Use:   "binlog-list",
	Short: binlogListShortDescription,
	Args:  cobra.NoArgs,
	PreRun: func(cmd *cobra.Command, args []string) {
		err := internal.AssertRequiredSettingsSet()
		if err != nil {
			tracelog.ErrorLogger.Printf("Configuration error: %v", err)
			tracelog.ErrorLogger.Println("Please ensure storage is configured via environment variables or config file")
			tracelog.ErrorLogger.FatalOnError(err)
		}
	},
	Run: func(cmd *cobra.Command, args []string) {
		storage, err := internal.ConfigureStorage()
		if err != nil {
			tracelog.ErrorLogger.Printf("Failed to configure storage: %v", err)
			tracelog.ErrorLogger.Println("Please check your storage configuration settings")
			tracelog.ErrorLogger.FatalOnError(err)
		}
		mysql.HandleBinlogList(storage.RootFolder(), listSince, listUntil)
	},
}

func init() {
	binlogListCmd.PersistentFlags().StringVar(&listSince, "since", "", "show binlogs modified since this time (e.g., '2h', '30m', '2023-01-01T10:00:00Z')")
	binlogListCmd.PersistentFlags().StringVar(&listUntil, "until", "", "show binlogs modified until this time (e.g., '1h', '2023-01-01T12:00:00Z')")
	cmd.AddCommand(binlogListCmd)
}
