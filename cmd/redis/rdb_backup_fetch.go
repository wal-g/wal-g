package redis

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	redisdb "github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/internal/databases/redis/ts"
	"github.com/wal-g/wal-g/utility"
)

const (
	backupFetchShortDescription   = "Fetches a Redis backup from storage"
	SkipCleanFlag                 = "skip-clean"
	SkipCleanShorthand            = "s"
	SkipBackupDownloadFlag        = "skip-backup-download"
	SkipBackupDownloadDescription = "Skip backup download"
	SkipChecksFlag                = "skip-checks"
	SkipChecksDescription         = "Skip checking Redis version compatibility with the backup"
	redisVersionFlag              = "redis-version"
)

var (
	skipClean              bool
	skipBackupDownloadFlag bool
	skipCheckFlag          bool
	redisVersion           string
	tsFetchBackup          string
)

var backupFetchCmd = &cobra.Command{
	Use:     "backup-fetch backup-name",
	Short:   backupFetchShortDescription,
	Args:    cobra.ExactArgs(1),
	PreRunE: validateBackupFetch,
	RunE:    runBackupFetch,
}

func validateBackupFetch(cmd *cobra.Command, args []string) error {
	if err := validateBackupType(cmd, args); err != nil {
		return err
	}
	if (backupType == aofBackupType || backupType == aofTSBackupType) && redisVersion == "" {
		return fmt.Errorf("--%s is required for backup type %q", redisVersionFlag, backupType)
	}
	if err := validateTSBackupFetchInput(); err != nil {
		return err
	}
	if backupType == rdbBackupType || backupType == rdbTSBackupType {
		conf.RequiredSettings[conf.NameStreamRestoreCmd] = true
		return internal.AssertRequiredSettingsSet()
	}
	return nil
}

func validateTSBackupFetchInput() error {
	switch backupType {
	case rdbBackupType, aofBackupType:
		if tsFetchBackup != "" {
			return fmt.Errorf("--%s is only valid for tiered-storage backup types", tsBackupFlag)
		}
	case rdbTSBackupType, aofTSBackupType, tsBackupType:
		if tsFetchBackup == "" {
			return fmt.Errorf("--%s is required for backup type %q", tsBackupFlag, backupType)
		}
	}
	return nil
}

func runBackupFetch(cmd *cobra.Command, args []string) error {
	internal.ConfigureLimiters()

	switch backupType {
	case rdbBackupType:
		return runRDBBackupFetch(cmd.Context(), args[0])
	case aofBackupType:
		return runAOFBackupFetch(cmd.Context(), args[0])
	case rdbTSBackupType:
		if err := runRDBBackupFetch(cmd.Context(), args[0]); err != nil {
			return err
		}
		return runTSBackupFetch(cmd.Context(), args[0])
	case aofTSBackupType:
		if err := runAOFBackupFetch(cmd.Context(), args[0]); err != nil {
			return err
		}
		return runTSBackupFetch(cmd.Context(), args[0])
	case tsBackupType:
		return runTSBackupFetch(cmd.Context(), args[0])
	default:
		return fmt.Errorf("unsupported redis backup type %q", backupType)
	}
}

func runRDBBackupFetch(ctx context.Context, backupName string) error {
	storage, err := internal.ConfigureStorage(ctx)
	if err != nil {
		return err
	}

	restoreCmd, err := internal.GetCommandSettingContext(ctx, conf.NameStreamRestoreCmd)
	if err != nil {
		return err
	}

	redisPassword, ok := conf.GetSetting(conf.RedisPassword)
	if ok && redisPassword != "" {
		restoreCmd.Env = append(restoreCmd.Env, fmt.Sprintf("REDISCLI_AUTH=%s", redisPassword))
	}
	restoreCmd.Stdout = os.Stdout
	restoreCmd.Stderr = os.Stderr

	return redisdb.HandleBackupFetch(ctx, storage.RootFolder(), backupName, restoreCmd, skipClean)
}

func runTSBackupFetch(ctx context.Context, backupName string) error {
	storage, err := internal.ConfigureStorage(ctx)
	if err != nil {
		return err
	}
	dataPrefix := backupName
	if backupType != tsBackupType {
		dataPrefix = ts.AttachedDataPrefix(backupName)
	}
	return ts.Fetch(ctx, ts.FetchArgs{
		Folder:     storage.RootFolder().GetSubFolder(utility.BaseBackupPath),
		DataPrefix: dataPrefix,
		TargetDir:  tsFetchBackup,
		SkipClean:  skipClean,
	})
}

func init() {
	backupFetchCmd.Flags().BoolVarP(&skipClean, SkipCleanFlag, SkipCleanShorthand, false, "Skip data folder clean check")
	backupFetchCmd.Flags().BoolVar(&skipBackupDownloadFlag, SkipBackupDownloadFlag, false, SkipBackupDownloadDescription)
	backupFetchCmd.Flags().BoolVar(&skipCheckFlag, SkipChecksFlag, false, SkipChecksDescription)
	backupFetchCmd.Flags().StringVar(&redisVersion, redisVersionFlag, "", "Redis version for AOF backup compatibility checks")
	backupFetchCmd.Flags().StringVar(&tsFetchBackup, tsBackupFlag, "", "Tiered-storage restore directory")
	backupFetchCmd.Flags().StringVarP(&backupType, typeFlag, typeShorthand, rdbBackupType,
		"Backup type: rdb, aof, rdb_ts, aof_ts, or ts")
	cmd.AddCommand(backupFetchCmd)
}
