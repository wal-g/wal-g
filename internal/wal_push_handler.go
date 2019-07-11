package internal

import (
	"bytes"
	"fmt"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
	"io/ioutil"
	"os"
	"path/filepath"
)

type CantOverwriteWalFileError struct {
	error
}

func NewCantOverwriteWalFileError(walFilePath string) CantOverwriteWalFileError {
	return CantOverwriteWalFileError{errors.Errorf("WAL file '%s' already archived, contents differ, unable to overwrite", walFilePath)}
}

func (err CantOverwriteWalFileError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// TODO : unit tests
// HandleWALPush is invoked to perform wal-g wal-push
func HandleWALPush(uploader *Uploader, walFilePath string) {
	if isWalAlreadyUploaded(uploader, walFilePath) {
		err := unmarkWalFile(uploader, walFilePath)

		if err != nil {
			tracelog.ErrorLogger.Printf("unmark wal-g status for %s file failed due following error %+v", walFilePath, err)
		}
		return
	}

	uploader.UploadingFolder = uploader.UploadingFolder.GetSubFolder(utility.WalPath)

	concurrency, err := GetMaxUploadConcurrency()
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}

	preventWalOverwrite := viper.GetBool(PreventWalOverwriteSetting)

	bgUploader := NewBgUploader(walFilePath, int32(concurrency-1), uploader, preventWalOverwrite)
	// Look for new WALs while doing main upload
	bgUploader.Start()
	err = UploadWALFile(uploader, walFilePath, bgUploader.preventWalOverwrite)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("%+v\n", err)
	}

	bgUploader.Stop()
	if uploader.getUseWalDelta() {
		uploader.deltaFileManager.FlushFiles(uploader.Clone())
	}
} //

// TODO : unit tests
// uploadWALFile from FS to the cloud
func UploadWALFile(uploader *Uploader, walFilePath string, preventWalOverwrite bool) error {
	if preventWalOverwrite {
		overwriteAttempt, err := checkWALOverwrite(uploader, walFilePath)
		if err != nil {
			return errors.Wrap(err, "Couldn't check whether there is an overwrite attempt due to inner error")
		} else if overwriteAttempt {
			return NewCantOverwriteWalFileError(walFilePath)
		}
	}
	walFile, err := os.Open(walFilePath)
	path, _ := filepath.Abs(walFilePath)
	if err != nil {
		return errors.Wrapf(err, "upload: could not open '%s'\n", path)
	}
	if err = uploader.UploadWalFile(walFile); err != nil {
		return errors.Wrapf(err, "upload: could not Upload '%s'\n", path)
	}
	return nil
}

// TODO : unit tests
func checkWALOverwrite(uploader *Uploader, walFilePath string) (overwriteAttempt bool, err error) {
	walFileReader, err := DownloadAndDecompressWALFile(uploader.UploadingFolder, filepath.Base(walFilePath)+"."+uploader.Compressor.FileExtension())
	if err != nil {
		if _, ok := err.(ArchiveNonExistenceError); ok {
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
		return true, nil
	} else {
		tracelog.WarningLogger.Printf("WAL file '%s' already archived, archived content equals\n", walFilePath)
		return false, nil
	}
}
