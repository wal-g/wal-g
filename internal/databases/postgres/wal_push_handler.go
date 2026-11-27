package postgres

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	conf "github.com/wal-g/wal-g/internal/config"
	"github.com/wal-g/wal-g/internal/statistics"
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

// SkipListedWAL reports whether a valid WALG_PG_ARCHIVE_SKIP_LIST names walFilePath, which must
// then be acknowledged without archiving. Call it before preparing an uploader: preparation
// fails when no storage is alive, and that is when such a list exists.
func SkipListedWAL(walFilePath string) bool {
	listPath := viper.GetString(conf.PgArchiveSkipList)
	if listPath == "" {
		return false
	}
	names, err := readWALSkipList(listPath)
	if err != nil {
		if !os.IsNotExist(err) {
			tracelog.WarningLogger.Printf("ignoring WAL skip list %s: %v", listPath, err)
		}
		return false
	}
	name := filepath.Base(walFilePath)
	if !slices.Contains(names, name) {
		return false
	}
	tracelog.WarningLogger.Printf("skipping archiving of %s: listed in %s", name, listPath)
	statistics.WalgMetrics.WalPushSkippedTotal.Inc()
	return true
}

// readWALSkipList returns no names once the list has outlived its TTL.
func readWALSkipList(listPath string) ([]string, error) {
	ttl, err := conf.GetDurationSetting(conf.PgArchiveSkipListTTL)
	if err != nil {
		return nil, err
	}
	info, err := os.Stat(listPath)
	if err != nil {
		return nil, err
	}
	if !time.Now().Before(info.ModTime().Add(ttl)) {
		return nil, nil
	}
	list, err := os.ReadFile(listPath)
	if err != nil {
		return nil, err
	}
	return strings.Split(string(list), "\n"), nil
}

// TODO : unit tests
// HandleWALPush is invoked to perform wal-g wal-push
func HandleWALPush(ctx context.Context, uploader *WalUploader, walFilePath string) (retErr error) {
	if uploader.ArchiveStatusManager.IsWalAlreadyUploaded(walFilePath) {
		if err := uploader.ArchiveStatusManager.UnmarkWalFile(walFilePath); err != nil {
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

	uploadStart := time.Now()
	bgUploader := NewBgUploader(ctx, walFilePath, int32(concurrency-1), totalBgUploadedLimit-1, uploader, preventWalOverwrite, readyRename)
	// Look for new WALs while doing main upload
	bgUploader.Start()
	defer func() {
		if err := bgUploader.Stop(); err != nil && retErr == nil {
			retErr = err
		}
	}()

	if err := uploadWALFile(ctx, uploader, walFilePath, preventWalOverwrite); err != nil {
		return err
	}
	if err := uploadLocalWalMetadata(ctx, walFilePath, uploader.Uploader); err != nil {
		return err
	}
	// bg uploads record into DeltaFileManager, drain them before flushing it
	if err := bgUploader.Stop(); err != nil {
		return err
	}
	statistics.WriteS3UploadTimeMetric(time.Since(uploadStart))

	if uploader.getUseWalDelta() {
		uploader.FlushFiles(ctx)
	}
	return nil
}

// TODO : unit tests
// uploadWALFile from FS to the cloud
func uploadWALFile(ctx context.Context, uploader *WalUploader, walFilePath string, preventWalOverwrite bool) (retErr error) {
	if preventWalOverwrite {
		overwriteAttempt, err := checkWALOverwrite(ctx, uploader, walFilePath)
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
	defer func() {
		if err := walFile.Close(); retErr == nil {
			retErr = err
		}
	}()
	err = uploader.UploadWalFile(ctx, walFile)
	if err != nil {
		return errors.Wrapf(err, "upload: could not Upload '%s'\n", walFilePath)
	}
	return nil
}

// TODO : unit tests
func checkWALOverwrite(ctx context.Context, uploader *WalUploader, walFilePath string) (overwriteAttempt bool, err error) {
	walFileReader, err := internal.DownloadAndDecompressStorageFile(ctx,
		internal.NewFolderReader(uploader.Folder()), filepath.Base(walFilePath))
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
