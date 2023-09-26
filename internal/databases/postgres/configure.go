package postgres

import (
	"fmt"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/fsutil"
	"github.com/wal-g/wal-g/internal/multistorage"
	"github.com/wal-g/wal-g/internal/multistorage/cache"
	"github.com/wal-g/wal-g/pkg/storages/storage"
)

// ConfigureMultiStorageFolder configures the primary storage folder and all the failover ones, if any, and builds a
// multi-storage folder that aggregates them. It also sets up a cache to keep storage alive check results there.
// This function doesn't set any specific multi-storage folder policies, so policies.Default are used initially.
// checkWrite should be true for operations supposes writing to the storage. It affects selecting R/O or R/W aliveness check.
func ConfigureMultiStorageFolder(checkWrite bool) (storage.Folder, error) {
	primaryStorage, err := internal.ConfigureFolder()
	if err != nil {
		return nil, fmt.Errorf("configure primary folder: %w", err)
	}

	failoverStorages, err := internal.InitFailoverStorages()
	if err != nil {
		return nil, fmt.Errorf("configure failover folders: %w", err)
	}

	cacheLifetime, err := internal.GetDurationSetting(internal.PgFailoverStorageCacheLifetime)
	if err != nil {
		return nil, fmt.Errorf("get failover storage cache lifetime setting: %w", err)
	}

	aliveChecker, err := configureStorageAliveChecker(checkWrite)
	if err != nil {
		return nil, fmt.Errorf("configure storage alive checker: %w", err)
	}
	statusCache, err := cache.NewStatusCache(
		primaryStorage,
		failoverStorages,
		cacheLifetime,
		aliveChecker,
	)
	if err != nil {
		return nil, fmt.Errorf("init cache with storage statuses: %w", err)
	}

	return multistorage.NewFolder(statusCache), nil
}

func configureStorageAliveChecker(checkWrite bool) (cache.AliveChecker, error) {
	aliveCheckTimeout, err := internal.GetDurationSetting(internal.PgFailoverStoragesCheckTimeout)
	if err != nil {
		return cache.AliveChecker{}, fmt.Errorf("get failover storage check timeout setting: %w", err)
	}

	if checkWrite {
		aliveCheckSize := viper.GetSizeInBytes(internal.PgFailoverStoragesCheckSize)
		return cache.NewRWAliveChecker(aliveCheckTimeout, uint32(aliveCheckSize)), nil
	}

	return cache.NewReadAliveChecker(aliveCheckTimeout), nil
}

// ConfigureWalUploader connects to storage and creates an uploader. It makes sure
// that a valid session has started; if invalid, returns AWS error
// and `<nil>` values.
func ConfigureWalUploader(baseUploader internal.Uploader) (uploader *WalUploader, err error) {
	useWalDelta, deltaDataFolder, err := configureWalDeltaUsage()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure WAL Delta usage")
	}

	var deltaFileManager *DeltaFileManager
	if useWalDelta {
		deltaFileManager = NewDeltaFileManager(deltaDataFolder)
	}

	uploader = NewWalUploader(baseUploader, deltaFileManager)
	return uploader, err
}

// TODO : unit tests
func configureWalDeltaUsage() (useWalDelta bool, deltaDataFolder fsutil.DataFolder, err error) {
	useWalDelta = viper.GetBool(internal.UseWalDeltaSetting)
	if !useWalDelta {
		return
	}
	dataFolderPath := internal.GetDataFolderPath()
	deltaDataFolder, err = fsutil.NewDiskDataFolder(dataFolderPath)
	if err != nil {
		useWalDelta = false
		tracelog.WarningLogger.Printf("can't use wal delta feature because can't open delta data folder '%s'"+
			" due to error: '%v'\n", dataFolderPath, err)
		err = nil
	}
	return
}

func getStopBackupTimeoutSetting() (time.Duration, error) {
	if !viper.IsSet(internal.PgStopBackupTimeout) {
		return 0, nil
	}

	timeout, err := internal.GetDurationSetting(internal.PgStopBackupTimeout)
	if err != nil {
		return 0, err
	}

	return timeout, nil
}
