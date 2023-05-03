package postgres

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/wal-g/wal-g/internal"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
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
func HandleWALPush(uploader *WalUploader, walFilePath string) error {
	if uploader.ArchiveStatusManager.IsWalAlreadyUploaded(walFilePath) {
		err := uploader.ArchiveStatusManager.UnmarkWalFile(walFilePath)

		if err != nil {
			tracelog.ErrorLogger.Printf("unmark wal-g status for %s file failed due following error %+v", walFilePath, err)
		}
		return uploadLocalWalMetadata(walFilePath, uploader)
	}

	concurrency, err := internal.GetMaxUploadConcurrency()
	if err != nil {
		return err
	}

	totalBgUploadedLimit := viper.GetInt32(internal.TotalBgUploadedLimit)
	// .history files must not be overwritten, see https://github.com/wal-g/wal-g/issues/420
	preventWalOverwrite := viper.GetBool(internal.PreventWalOverwriteSetting) || strings.HasSuffix(walFilePath, ".history")
	readyRename := viper.GetBool(internal.PgReadyRename)

	bgUploader := NewBgUploader(walFilePath, int32(concurrency-1), totalBgUploadedLimit-1, uploader, preventWalOverwrite, readyRename)
	// Look for new WALs while doing main upload
	bgUploader.Start()

	err = uploadWALFile(uploader, walFilePath, bgUploader.preventWalOverwrite)
	if err != nil {
		return err
	}
	err = uploadLocalWalMetadata(walFilePath, uploader.Uploader)
	if err != nil {
		return err
	}

	err = bgUploader.Stop()
	if err != nil {
		return err
	}

	if uploader.getUseWalDelta() {
		uploader.FlushFiles()
	}
	return nil
}

// TODO : unit tests
// uploadWALFile from FS to the cloud
func uploadWALFile(uploader *WalUploader, walFilePath string, preventWalOverwrite bool) error {
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
	return errors.Wrapf(err, "upload: could not Upload '%s'\n", walFilePath)
}

// TODO : unit tests
func checkWALOverwrite(uploader *WalUploader, walFilePath string) (overwriteAttempt bool, err error) {
	walFileReader, err := internal.DownloadAndDecompressStorageFile(internal.NewFolderReader(uploader.Folder()), filepath.Base(walFilePath))
	if err != nil {
		if _, ok := err.(internal.ArchiveNonExistenceError); ok {
			err = nil
		}
		return false, err
	}

	archived, err := io.ReadAll(walFileReader)
	if err != nil {
		return false, err
	}

	localBytes, err := os.ReadFile(walFilePath)
	if err != nil {
		return false, err
	}

	if !bytes.Equal(archived, localBytes) {
		return true, newCantOverwriteWalFileError(walFilePath)
	}
	tracelog.InfoLogger.Printf("WAL file '%s' already archived with equal content, skipping", walFilePath)
	return true, nil
}
