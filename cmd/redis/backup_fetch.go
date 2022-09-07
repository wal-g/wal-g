package redis

import (
	"context"
	"fmt"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/utility"
)

const backupFetchShortDescription = "Fetches desired backup from storage"

var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch backup-name",
	Short: backupFetchShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		folder, err := internal.ConfigureFolder()
		tracelog.ErrorLogger.FatalOnError(err)

		restoreCmd, err := internal.GetCommandSettingContext(ctx, internal.NameStreamRestoreCmd)
		tracelog.ErrorLogger.FatalOnError(err)

		redisPassword, ok := internal.GetSetting(internal.RedisPassword)
		if ok && redisPassword != "" { // special hack for redis-cli
			restoreCmd.Env = append(restoreCmd.Env, fmt.Sprintf("REDISCLI_AUTH=%s", redisPassword))
		}
		restoreCmd.Stdout = os.Stdout
		restoreCmd.Stderr = os.Stderr

		err = redis.HandleBackupFetch(ctx, folder, args[0], restoreCmd)
		tracelog.ErrorLogger.FatalOnError(err)
	},
}

func init() {
	cmd.AddCommand(backupFetchCmd)
}
