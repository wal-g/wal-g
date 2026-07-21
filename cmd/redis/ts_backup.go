package redis

import (
	"context"
	"fmt"
	"os"

	"golang.org/x/sync/errgroup"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	redisdb "github.com/wal-g/wal-g/internal/databases/redis"
	"github.com/wal-g/wal-g/internal/databases/redis/aof"
	"github.com/wal-g/wal-g/internal/databases/redis/archive"
	client "github.com/wal-g/wal-g/internal/databases/redis/client"
	"github.com/wal-g/wal-g/internal/databases/redis/rdb"
	"github.com/wal-g/wal-g/internal/databases/redis/ts"
	"github.com/wal-g/wal-g/utility"
)

func runTieredStorageBackupPush(ctx context.Context) error {
	if backupType == tsBackupType {
		return runStandaloneTSBackupPush(ctx)
	}

	var backupName string
	switch backupType {
	case rdbTSBackupType:
		backupName = rdb.GenerateNewBackupName()
	case aofTSBackupType:
		backupName = aof.GenerateNewBackupName()
	default:
		return fmt.Errorf("unsupported tiered-storage backup type %q", backupType)
	}

	mainUploader, err := newBaseBackupUploader(ctx)
	if err != nil {
		return err
	}
	tsUploader, err := newBaseBackupUploader(ctx)
	if err != nil {
		return err
	}

	mainMeta := newMainMetaConstructor(mainUploader, backupType)
	pinFolder, _ := conf.GetSetting(conf.RedisTSPinFolder)
	var tsBackupMeta *archive.Backup

	g, groupCtx := errgroup.WithContext(ctx)
	g.Go(func() error {
		switch backupType {
		case rdbTSBackupType:
			return runDeferredRDBBackupPush(groupCtx, mainUploader, mainMeta, backupName)
		case aofTSBackupType:
			return runDeferredAOFBackupPush(groupCtx, mainUploader, mainMeta, backupName)
		default:
			return fmt.Errorf("unsupported tiered-storage backup type %q", backupType)
		}
	})
	g.Go(func() error {
		backup, pushErr := ts.Push(groupCtx, ts.PushArgs{
			Uploader:      tsUploader,
			SourceDir:     tsBackup,
			PinFolder:     pinFolder,
			DataPrefix:    ts.AttachedDataPrefix(backupName),
			BackupID:      tsBackupID,
			BackupPath:    tsBackup,
			Permanent:     permanent,
			DeferSentinel: true,
		})
		if pushErr == nil {
			tsBackupMeta = backup
		}
		return pushErr
	})

	if err = g.Wait(); err != nil {
		cleanupErr := internal.DeleteBackups(ctx, mainUploader.Folder(), []string{backupName})
		if cleanupErr != nil {
			return fmt.Errorf("tiered-storage backup failed: %w; cleanup %s: %v", err, backupName, cleanupErr)
		}
		return fmt.Errorf("tiered-storage backup failed: %w", err)
	}

	mainBackup, ok := mainMeta.MetaInfo().(*archive.Backup)
	if !ok {
		return fmt.Errorf("unexpected main backup sentinel type")
	}
	if err = internal.UploadSentinel(ctx, mainUploader, mainBackup, backupName); err != nil {
		_ = internal.DeleteBackups(ctx, mainUploader.Folder(), []string{backupName})
		return fmt.Errorf("upload main backup sentinel: %w", err)
	}
	if err = internal.UploadSentinel(ctx, tsUploader, tsBackupMeta, ts.AttachedDataPrefix(backupName)); err != nil {
		_ = internal.DeleteBackups(ctx, mainUploader.Folder(), []string{backupName})
		return fmt.Errorf("upload tiered-storage sentinel: %w", err)
	}
	return nil
}

func runStandaloneTSBackupPush(ctx context.Context) error {
	uploader, err := newBaseBackupUploader(ctx)
	if err != nil {
		return err
	}
	backupName := ts.StandaloneDataPrefix()
	pinFolder, _ := conf.GetSetting(conf.RedisTSPinFolder)
	if _, err = ts.Push(ctx, ts.PushArgs{
		Uploader:   uploader,
		SourceDir:  tsBackup,
		PinFolder:  pinFolder,
		DataPrefix: backupName,
		BackupID:   tsBackupID,
		BackupPath: tsBackup,
		Permanent:  permanent,
	}); err != nil {
		_ = internal.DeleteBackups(ctx, uploader.Folder(), []string{backupName})
		return err
	}
	return nil
}

func newBaseBackupUploader(ctx context.Context) (internal.Uploader, error) {
	uploader, err := internal.ConfigureUploader(ctx)
	if err != nil {
		return nil, err
	}
	uploader.ChangeDirectory(utility.BaseBackupPath)
	return uploader, nil
}

func newMainMetaConstructor(uploader internal.Uploader, kind string) internal.MetaConstructor {
	processName, _ := conf.GetSetting(conf.RedisServerProcessName)
	var versionParser *archive.VersionParser
	if kind == aofTSBackupType {
		versionParser = archive.NewVersionParser(processName)
	}
	mainType := archive.RDBBackupType
	if kind == aofTSBackupType {
		mainType = archive.AOFBackupType
	}
	return archive.NewBackupRedisMetaConstructor(
		uploader.Folder(), permanent, mainType, versionParser, client.NewServerDataGetter(),
	)
}

func runDeferredRDBBackupPush(ctx context.Context, uploader internal.Uploader, meta internal.MetaConstructor, backupName string) error {
	backupCmd, err := internal.GetCommandSettingContext(ctx, conf.NameStreamCreateCmd)
	if err != nil {
		return err
	}
	if redisPassword, ok := conf.GetSetting(conf.RedisPassword); ok && redisPassword != "" {
		backupCmd.Env = append(backupCmd.Env, fmt.Sprintf("REDISCLI_AUTH=%s", redisPassword))
	}
	backupCmd.Stderr = os.Stderr
	return redisdb.HandleRDBBackupPush(ctx, redisdb.RDBBackupPushArgs{
		BackupCmd:       backupCmd,
		BackupName:      backupName,
		Sharded:         sharded,
		Uploader:        uploader,
		MetaConstructor: meta,
		DeferSentinel:   true,
	})
}

func runDeferredAOFBackupPush(ctx context.Context, uploader internal.Uploader, meta internal.MetaConstructor, backupName string) error {
	return redisdb.HandleAOFBackupPush(ctx, redisdb.AOFBackupPushArgs{
		BackupName:      backupName,
		Sharded:         sharded,
		Uploader:        uploader,
		MetaConstructor: meta,
		DeferSentinel:   true,
	})
}
