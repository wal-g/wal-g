package pg

import (
	"time"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/policies"
)

const (
	walSendShortDescription = "Serve WAL from the storage to connecting standbys " +
		"via the postgres Streaming Replication Protocol"

	walSendAddressFlag      = "address"
	walSendPollIntervalFlag = "poll-interval"
	walSendAddressHelp      = "Address to listen replication connections on. " +
		"Defaults to the loopback interface: the server serves WAL with trust auth " +
		"and no TLS, so remote exposure requires an explicit address"
	walSendPollIntervalHelp = "How often to poll the storage for new WAL segments " +
		"and send keepalive messages to idle clients"
)

var (
	walSendAddress      string
	walSendPollInterval time.Duration
)

// walSendCmd represents the wal-send command
var walSendCmd = &cobra.Command{
	Use:   "wal-send",
	Short: walSendShortDescription,
	Args:  cobra.ExactArgs(0),
	Run: func(cmd *cobra.Command, args []string) {
		storage, err := internal.ConfigureMultiStorage(cmd.Context(), false)
		tracelog.ErrorLogger.FatalOnError(err)

		rootFolder := multistorage.SetPolicies(storage.RootFolder(), policies.UniteAllStorages)
		rootFolder, err = multistorage.UseAllAliveStorages(cmd.Context(), rootFolder)
		tracelog.ErrorLogger.FatalOnError(err)
		tracelog.InfoLogger.Printf("WAL will be served from storages: %v", multistorage.UsedStorages(rootFolder))

		err = postgres.HandleWALSend(cmd.Context(), rootFolder, postgres.WalSendOptions{
			Address:      walSendAddress,
			PollInterval: walSendPollInterval,
		})
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	walSendCmd.Flags().StringVar(&walSendAddress, walSendAddressFlag, "127.0.0.1:8000", walSendAddressHelp)
	walSendCmd.Flags().DurationVar(&walSendPollInterval, walSendPollIntervalFlag, 5*time.Second,
		walSendPollIntervalHelp)

	Cmd.AddCommand(walSendCmd)
}
