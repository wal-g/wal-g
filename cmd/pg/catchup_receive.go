package pg

import (
	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"strconv"
)

const (
	CatchupReceiveShortDescription = "Receive an incremental backup from another instance"
)

// catchupFetchCmd represents the catchup-fetch command
var catchupReceiveCmd = &cobra.Command{
	Use:   "catchup-receive PGDATA port_number",
	Short: CatchupReceiveShortDescription,
	Args:  cobra.ExactArgs(2),
	Run: func(cmd *cobra.Command, args []string) {
		port, err := strconv.Atoi(args[1])
		tracelog.ErrorLogger.FatalOnError(err)
		postgres.HandleCatchupReceive(args[0], port)
	},
	Annotations: map[string]string{"NoStorage": ""},
}

func init() {
	Cmd.AddCommand(catchupReceiveCmd)
}
