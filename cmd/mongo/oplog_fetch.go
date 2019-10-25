package mongo

import (
	"github.com/spf13/cobra"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/mongo"
)

const OplogFetchShortDescription = "Fetches OpLogs from storage"
const sinceFlagShortDescription  = "backup name starting from which you want to take binlog"

var backupName string

// backupFetchCmd represents the streamFetch command
var oplogFetchCmd = &cobra.Command{
	Use:   "oplog-fetch",
	Short: OplogFetchShortDescription,
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)
		tracelog.ErrorLogger.FatalfOnError("Failed to parse until timestamp ", err, )
		tracelog.ErrorLogger.FatalOnError(mongo.HandleOplogFetch(folder, backupName, ))
	},
}

func init() {
	oplogFetchCmd.PersistentFlags().StringVar(&backupName, "since", "LATEST", sinceFlagShortDescription)
	Cmd.AddCommand(oplogFetchCmd)
}