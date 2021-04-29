package postgres

import (
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/fsutil"
)

// ConfigureWalUploader connects to storage and creates an uploader. It makes sure
// that a valid session has started; if invalid, returns AWS error
// and `<nil>` values.
func ConfigureWalUploader() (uploader *WalUploader, err error) {
	uploader, err = ConfigureWalUploaderWithoutCompressMethod()
	if err != nil {
		return nil, err
	}

	folder := uploader.UploadingFolder
	deltaFileManager := uploader.DeltaFileManager

	compressor, err := internal.ConfigureCompressor()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure compression")
	}

	uploader = NewWalUploader(compressor, folder, deltaFileManager)
	return uploader, err
}

func ConfigureWalUploaderWithoutCompressMethod() (uploader *WalUploader, err error) {
	folder, err := internal.ConfigureFolder()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure folder")
	}

	useWalDelta, deltaDataFolder, err := configureWalDeltaUsage()
	if err != nil {
		return nil, errors.Wrap(err, "failed to configure WAL Delta usage")
	}

	var deltaFileManager *DeltaFileManager = nil
	if useWalDelta {
		deltaFileManager = NewDeltaFileManager(deltaDataFolder)
	}

	uploader = NewWalUploader(nil, folder, deltaFileManager)
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
