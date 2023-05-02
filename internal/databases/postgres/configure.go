package postgres

import (
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/fsutil"
)

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
