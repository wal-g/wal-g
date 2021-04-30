package postgres

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/wal-g/wal-g/internal"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type CantOverwriteWalFileError struct {
	error
}

func newCantOverwriteWalFileError(walFilePath string) CantOverwriteWalFileError {
	return CantOverwriteWalFileError{
		errors.Errorf("WAL file '%s' already archived, contents differ, unable to overwrite",
			walFilePath)}
}

func (err CantOverwriteWalFileError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TODO : unit tests
// HandleWALPush is invoked to perform wal-g wal-push
func HandleWALPush(uploader *WalUploader, walFilePath string) {
	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.WalPath)
	if uploader.ArchiveStatusManager.IsWalAlreadyUploaded(walFilePath) {
		err := uploader.ArchiveStatusManager.UnmarkWalFile(walFilePath)

		if err != nil {
			tracelog.ErrorLogger.Printf("unmark wal-g status for %s file failed due following error %+v", walFilePath, err)
		}
		err = uploadLocalWalMetadata(walFilePath, uploader.Uploader)
		tracelog.ErrorLogger.FatalOnError(err)
		return
	}

	concurrency, err := internal.GetMaxUploadConcurrency()
	tracelog.ErrorLogger.FatalOnError(err)

	totalBgUploadedLimit := viper.GetInt32(internal.TotalBgUploadedLimit)
	preventWalOverwrite := viper.GetBool(internal.PreventWalOverwriteSetting)
	readyRename := viper.GetBool(internal.PGReadyRename)

	bgUploader := NewBgUploader(walFilePath, int32(concurrency-1), totalBgUploadedLimit-1, uploader, preventWalOverwrite, readyRename)
	// Look for new WALs while doing main upload
	bgUploader.Start()

	// do not rename the status file for the first WAL segment in a batch
	// to avoid flooding the PostgreSQL logs with unnecessary warnings
	err = uploadWALFile(uploader, walFilePath, bgUploader.preventWalOverwrite, false)
	tracelog.ErrorLogger.FatalOnError(err)
	err = uploadLocalWalMetadata(walFilePath, uploader.Uploader)
	tracelog.ErrorLogger.FatalOnError(err)

	err = bgUploader.Stop()
	tracelog.ErrorLogger.FatalOnError(err)

	if uploader.getUseWalDelta() {
		uploader.FlushFiles()
	}
}

// TODO : unit tests
// uploadWALFile from FS to the cloud
func uploadWALFile(uploader *WalUploader, walFilePath string, preventWalOverwrite bool, ReadyRename bool) error {
	if preventWalOverwrite {
		overwriteAttempt, err := checkWALOverwrite(uploader, walFilePath)
		if overwriteAttempt {
			return err
		} else if err != nil {
			return errors.Wrap(err, "Couldn't check whether there is an overwrite attempt due to inner error")
		}
	}
	walFile, err := os.Open(walFilePath)
	if err != nil {
		return errors.Wrapf(err, "upload: could not open '%s'\n", walFilePath)
	}
	err = uploader.UploadWalFile(walFile)

	if err != nil {
		return errors.Wrapf(err, "upload: could not Upload '%s'\n", walFilePath)
	}

	// rename WAL status file ".ready" to ".done" if requested
	if ReadyRename && err == nil {

		var wALFileName = filepath.Base(walFilePath)
		var readyPath = filepath.Join(internal.GetPGArchiveStatusFolderPath(), wALFileName+".ready")
		var donePath = filepath.Join(internal.GetPGArchiveStatusFolderPath(), wALFileName+".done")

		// error here is not a fatal thing, just a bit more work for the next wal-push
		err = os.Rename(readyPath, donePath)
		tracelog.ErrorLogger.PrintOnError(err)
	}

	return nil
}

// TODO : unit tests
func checkWALOverwrite(uploader *WalUploader, walFilePath string) (overwriteAttempt bool, err error) {
	walFileReader, err := internal.DownloadAndDecompressStorageFile(uploader.UploadingFolder, filepath.Base(walFilePath))
	if err != nil {
		if _, ok := err.(internal.ArchiveNonExistenceError); ok {
			err = nil
		}
		return false, err
	}

	archived, err := ioutil.ReadAll(walFileReader)
	if err != nil {
		return false, err
	}

	localBytes, err := ioutil.ReadFile(walFilePath)
	if err != nil {
		return false, err
	}

	if !bytes.Equal(archived, localBytes) {
		return true, newCantOverwriteWalFileError(walFilePath)
	}
	tracelog.InfoLogger.Printf("WAL file '%s' already archived with equal content, skipping", walFilePath)
	return true, nil
}
