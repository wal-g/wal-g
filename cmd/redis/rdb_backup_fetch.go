package redis

import (
	"context"
	"fmt"
	"os"
	"syscall"

	"github.com/spf13/cobra"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/utility"
)

const backupFetchShortDescription = "Fetches desired rdb backup from storage"

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
		tracelog.ErrorLogger.FatalOnError(err)

		var cmdArgs []string
		redisUser, ok := conf.GetSetting(conf.RedisRestoreBackupACLUser)
		if ok && redisUser != "" {
			cmdArgs = append(cmdArgs, "--user", redisUser)
		}

		restoreCmd, err := internal.GetCommandSettingContext(ctx, conf.NameStreamRestoreCmd, cmdArgs...)
		tracelog.ErrorLogger.FatalOnError(err)
		tracelog.InfoLogger.Print(restoreCmd.String())

		redisPassword, ok := conf.GetSetting(conf.RedisPassword)
		if ok && redisPassword != "" { // special hack for redis-cli
			restoreCmd.Env = append(restoreCmd.Env, fmt.Sprintf("REDISCLI_AUTH=%s", redisPassword))
		}

		restoreCmd.Stdout = os.Stdout
		restoreCmd.Stderr = os.Stderr

		err = redis.HandleBackupFetch(ctx, storage.RootFolder(), args[0], restoreCmd)
		tracelog.ErrorLogger.FatalOnError(err)
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
	cmd.AddCommand(rdbBackupFetchCmd)
}
