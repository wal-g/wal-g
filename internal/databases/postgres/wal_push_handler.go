package postgres

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"

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
func HandleWALPush(ctx context.Context, uploader *WalUploader, walFilePath string) error {
	if uploader.ArchiveStatusManager.IsWalAlreadyUploaded(walFilePath) {
		err := uploader.ArchiveStatusManager.UnmarkWalFile(walFilePath)

		if err != nil {
			tracelog.ErrorLogger.Printf("unmark wal-g status for %s file failed due following error %+v", walFilePath, err)
		}
		return uploadLocalWalMetadata(ctx, walFilePath, uploader)
	}

	concurrency, err := conf.GetMaxUploadConcurrency()
	if err != nil {
		return err
	}

	totalBgUploadedLimit := viper.GetInt32(conf.TotalBgUploadedLimit)
	// .history files must not be overwritten, see https://github.com/wal-g/wal-g/issues/420
	preventWalOverwrite := viper.GetBool(conf.PreventWalOverwriteSetting) || strings.HasSuffix(walFilePath, ".history")
	readyRename := viper.GetBool(conf.PgReadyRename)

	bgUploader := NewBgUploader(ctx, walFilePath, int32(concurrency-1), totalBgUploadedLimit-1, uploader, preventWalOverwrite, readyRename)
	// Look for new WALs while doing main upload
	bgUploader.Start()

	err = uploadWALFile(ctx, uploader, walFilePath, preventWalOverwrite)
	if err != nil {
		return err
	}
	err = uploadLocalWalMetadata(ctx, walFilePath, uploader.Uploader)
	if err != nil {
		return err
	}

	err = bgUploader.Stop()
	if err != nil {
		return err
	}

	if uploader.getUseWalDelta() {
		// The `uploader.FlushFiles` method assumes that we have already read the WAL part files into
		// memory before calling it. Its first step is to delete the WAL part files from disk, then
		// write the latest in-memory WAL part file back to disk. However, since we are currently
		// handling a backup history file, it will not trigger the action of reading WAL part files
		// into memory. This will cause some WAL parts within the WAL part file to be lost. As a result:

		// 1. The WAL part file becomes incomplete (see WalPartFile.IsComplete())

		// 2. Due to the incomplete WAL parts, the WAL part file can never be marked as completed,
		//    and consequently its corresponding delta file will not be uploaded
		//    (see DeltaFileManager.FlushPartFiles(), DeltaFileManager.FlushDeltaFiles())

		// 3. When performing delta backup, since the required delta file is missing, it will fail to
		//    generate delta map and fall back to full backup

		// Therefore, we need to place `HandleBackupHistoryFile` before `FlushFiles` here to avoid
		// this issue.
		if err := uploader.HandleBackupHistoryFile(walFilePath); err != nil {
			tracelog.WarningLogger.Printf("handle backup history file failed due to following error %v", err)
		}
		uploader.FlushFiles(ctx)
	}
	return nil
}

// TODO : unit tests
// uploadWALFile from FS to the cloud
func uploadWALFile(ctx context.Context, uploader *WalUploader, walFilePath string, preventWalOverwrite bool) error {
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
	err = uploader.UploadWalFile(ctx, walFile)
	if err != nil {
		return errors.Wrapf(err, "upload: could not Upload '%s'\n", walFilePath)
	}
	return walFile.Close()
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
