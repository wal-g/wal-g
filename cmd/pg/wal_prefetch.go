package pg

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/postgres"
	"github.com/wal-g/wal-g/internal/multistorage"
)

const WalPrefetchShortDescription = `Used for prefetching process forking
and should not be called by user.`

// WalPrefetchCmd represents the walPrefetch command
var WalPrefetchCmd = &cobra.Command{
	Use:    "wal-prefetch wal_name prefetch_location",
	Short:  WalPrefetchShortDescription,
	Args:   cobra.ExactArgs(2),
	Hidden: true,
	Run: func(cmd *cobra.Command, args []string) {
		reconfigureLoggers()

		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		failover, err := internal.InitFailoverStorages()
		tracelog.ErrorLogger.FatalOnError(err)

		folderReader, err := multistorage.NewStorageFolderReader(folder, failover)
		tracelog.ErrorLogger.FatalOnError(err)

		err = postgres.HandleWALPrefetch(folderReader, args[0], args[1])
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	Cmd.AddCommand(WalPrefetchCmd)
}

// wal-prefetch (WalPrefetchCmd) is internal tool, so to avoid confusion about errors in restoration process
// we reconfigure loggers specially for internal use. All logs having PREFETCH prefix can be safely ignored
func reconfigureLoggers() {
	if viper.Get(internal.LogLevelSetting) == tracelog.DevelLogLevel {
		addPrefetchPrefixToAllLoggers()
		return
	}

	discardAllLoggers()
}

func discardAllLoggers() {
	tracelog.ErrorLogger.SetOutput(io.Discard)
	tracelog.WarningLogger.SetOutput(io.Discard)
	tracelog.InfoLogger.SetOutput(io.Discard)
	tracelog.DebugLogger.SetOutput(io.Discard)
}

func addPrefetchPrefixToAllLoggers() {
	tracelog.ErrorLogger.SetPrefix(fmt.Sprintf("PREFETCH %s", tracelog.ErrorLogger.Prefix()))
	tracelog.WarningLogger.SetPrefix(fmt.Sprintf("PREFETCH %s", tracelog.WarningLogger.Prefix()))
	tracelog.InfoLogger.SetPrefix(fmt.Sprintf("PREFETCH %s", tracelog.InfoLogger.Prefix()))
	tracelog.DebugLogger.SetPrefix(fmt.Sprintf("PREFETCH %s", tracelog.DebugLogger.Prefix()))
}
