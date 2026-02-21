package redis

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/internal/logging"
	"github.com/wal-g/wal-g/utility"
)

const (
	backupFetchShortDescription = "Fetches desired rdb backup from storage"
	SkipCleanFlag               = "skip-clean"
	SkipCleanShorthand          = "s"
)

var (
	skipClean bool
)

var backupFetchCmd = &cobra.Command{
	Use:   "backup-fetch backup-name",
	Short: backupFetchShortDescription,
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		internal.ConfigureLimiters()

		ctx, cancel := context.WithCancel(context.Background())
		signalHandler := utility.NewSignalHandler(ctx, cancel, []os.Signal{syscall.SIGINT, syscall.SIGTERM})
		defer func() { _ = signalHandler.Close() }()

		storage, err := internal.ConfigureStorage()
		logging.FatalOnError(err)

		var cmdArgs []string
		restoreCmd, err := internal.GetCommandSettingContext(ctx, conf.NameStreamRestoreCmd, cmdArgs...)
		logging.FatalOnError(err)
		slog.Info(restoreCmd.String())

		redisPassword, ok := conf.GetSetting(conf.RedisPassword)
		if ok && redisPassword != "" { // special hack for redis-cli
			restoreCmd.Env = append(restoreCmd.Env, fmt.Sprintf("REDISCLI_AUTH=%s", redisPassword))
		}

		restoreCmd.Stdout = os.Stdout
		restoreCmd.Stderr = os.Stderr

		err = redis.HandleBackupFetch(ctx, storage.RootFolder(), args[0], restoreCmd, skipClean)
		logging.FatalOnError(err)
	},
}

func init() {
	cmd.AddCommand(backupFetchCmd)

	rdbBackupFetchCmd := &cobra.Command{
		Use:   "rdb-backup-fetch backup-name",
		Short: backupFetchCmd.Short,
		Args:  backupFetchCmd.Args,
		Run:   backupFetchCmd.Run,
	}
	rdbBackupFetchCmd.Flags().BoolVarP(&skipClean, SkipCleanFlag, SkipCleanShorthand, false, "Skip data folder clean check")
	cmd.AddCommand(rdbBackupFetchCmd)
}
