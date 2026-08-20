package redis

import (
	"context"
	"fmt"
	"os"

	"github.com/spf13/cobra"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	redisdb "github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	client "github.com/wal-g/wal-g/internal/databases/redis/client"
	"github.com/wal-g/wal-g/utility"
)

var (
	permanent  bool
	sharded    bool
	backupType string
	tsBackup   string
	tsBackupID string
)

const (
	backupPushShortDescription = "Creates and uploads a Redis backup"
	PermanentFlag              = "permanent"
	PermanentShorthand         = "p"
	shardedFlag                = "sharded"
	shardedShorthand           = "s"
	typeFlag                   = "type"
	typeShorthand              = "t"
	tsBackupFlag               = "ts-backup"
	tsBackupIDFlag             = "ts-backup-id"

	rdbBackupType   = "rdb"
	aofBackupType   = "aof"
	rdbTSBackupType = "rdb_ts"
	aofTSBackupType = "aof_ts"
	tsBackupType    = "ts"
)

func validateBackupType(_ *cobra.Command, _ []string) error {
	switch backupType {
	case rdbBackupType, aofBackupType, rdbTSBackupType, aofTSBackupType, tsBackupType:
		return nil
	default:
		return fmt.Errorf("invalid --%s value %q: must be one of %s, %s, %s, %s, %s", typeFlag, backupType,
			rdbBackupType, aofBackupType, rdbTSBackupType, aofTSBackupType, tsBackupType)
	}
}

// backupPushCmd represents the Redis backup-push command.
var backupPushCmd = &cobra.Command{
	Use:     "backup-push",
	Short:   backupPushShortDescription,
	Args:    cobra.NoArgs,
	PreRunE: validateBackupPush,
	RunE:    runBackupPush,
}

func validateBackupPush(cmd *cobra.Command, args []string) error {
	if err := validateBackupType(cmd, args); err != nil {
		return err
	}
	if err := validateTSBackupPushInput(); err != nil {
		return err
	}
	if backupType == rdbBackupType || backupType == rdbTSBackupType {
		conf.RequiredSettings[conf.NameStreamCreateCmd] = true
		return internal.AssertRequiredSettingsSet()
	}
	return nil
}

func validateTSBackupPushInput() error {
	switch backupType {
	case rdbBackupType, aofBackupType:
		if tsBackup != "" || tsBackupID != "" {
			return fmt.Errorf("--%s and --%s are only valid for tiered-storage backup types", tsBackupFlag, tsBackupIDFlag)
		}
		return nil
	case rdbTSBackupType, aofTSBackupType, tsBackupType:
		if tsBackup == "" {
			return fmt.Errorf("--%s is required for backup type %q", tsBackupFlag, backupType)
		}
		info, err := os.Stat(tsBackup)
		if err != nil {
			return fmt.Errorf("stat --%s directory %s: %w", tsBackupFlag, tsBackup, err)
		}
		if !info.IsDir() {
			return fmt.Errorf("--%s path %s is not a directory", tsBackupFlag, tsBackup)
		}
		entries, err := os.ReadDir(tsBackup)
		if err != nil {
			return fmt.Errorf("read --%s directory %s: %w", tsBackupFlag, tsBackup, err)
		}
		if len(entries) == 0 {
			return fmt.Errorf("--%s directory %s is empty", tsBackupFlag, tsBackup)
		}
		return nil
	default:
		return nil
	}
}

func runBackupPush(cmd *cobra.Command, _ []string) error {
	internal.ConfigureLimiters()
	ctx := cmd.Context()

	switch backupType {
	case rdbBackupType:
		return runRDBBackupPush(ctx)
	case aofBackupType:
		return runAOFBackupPush(ctx)
	case rdbTSBackupType, aofTSBackupType, tsBackupType:
		return runTieredStorageBackupPush(ctx)
	default:
		return fmt.Errorf("unsupported redis backup type %q", backupType)
	}
}

func runRDBBackupPush(ctx context.Context) error {
	uploader, err := internal.ConfigureUploader(ctx)
	if err != nil {
		return err
	}
	uploader.ChangeDirectory(utility.BaseBackupPath)

	backupCmd, err := internal.GetCommandSettingContext(ctx, conf.NameStreamCreateCmd)
	if err != nil {
		return err
	}

	redisPassword, ok := conf.GetSetting(conf.RedisPassword)
	if ok && redisPassword != "" {
		backupCmd.Env = append(backupCmd.Env, fmt.Sprintf("REDISCLI_AUTH=%s", redisPassword))
	}
	backupCmd.Stderr = os.Stderr

	pushArgs := redisdb.RDBBackupPushArgs{
		BackupCmd: backupCmd,
		Sharded:   sharded,
		Uploader:  uploader,
		MetaConstructor: archive.NewBackupRedisMetaConstructor(
			uploader.Folder(), permanent, archive.RDBBackupType, nil, client.NewServerDataGetter(),
		),
	}
	return redisdb.HandleRDBBackupPush(ctx, pushArgs)
}

func init() {
	backupPushCmd.Flags().BoolVarP(&permanent, PermanentFlag, PermanentShorthand, false, "Push backup with the permanent flag")
	backupPushCmd.Flags().BoolVarP(&sharded, shardedFlag, shardedShorthand, false, "Push a sharded backup")
	backupPushCmd.Flags().StringVarP(&backupType, typeFlag, typeShorthand, rdbBackupType,
		"Backup type: rdb, aof, rdb_ts, aof_ts, or ts")
	backupPushCmd.Flags().StringVar(&tsBackup, tsBackupFlag, "", "Frozen tiered-storage backup directory")
	backupPushCmd.Flags().StringVar(&tsBackupID, tsBackupIDFlag, "", "Tiered-storage backup ID")
	cmd.AddCommand(backupPushCmd)
}
