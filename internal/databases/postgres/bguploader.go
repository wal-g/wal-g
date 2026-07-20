package postgres

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"time"

	"github.com/wal-g/tracelog"
	"golang.org/x/sync/semaphore"
)

const (
	// readySuffix is the filename suffix indicating WAL archives which are
	// ready for upload
	readySuffix = ".ready"

	// archiveStatusDir is the subdirectory containing status files of WAL
	// segments
	archiveStatusDir = "archive_status"

	// pollPauseDuration defines the amount of time to pause before scanning the
	// filesystem again to find WAL segments
	pollPauseDuration = 100 * time.Millisecond
)

// BgUploader represents the state of concurrent WAL upload
type BgUploader struct {
	// pg_[wals|xlog]
	dir string

	// uploading structure
	uploader *WalUploader

	preventWalOverwrite bool
	readyRename         bool

	// uploadCtx drives in-flight uploads, canceled only externally (eg Ctrl-C), not by Stop
	uploadCtx context.Context //nolint:containedctx // parent ctx for background WAL upload goroutines
	// ctx gates enqueueing of new uploads, canceled by Stop to halt scanning while in-flight uploads drain
	ctx context.Context //nolint:containedctx // gates background WAL upload enqueueing
	// cancelFunc signals internals to stop enqueuing more uploads
	cancelFunc context.CancelFunc

	// workerCountSem tracks number of concurrent uploaders. Limited to
	// maxParallelWorkers.
	workerCountSem *semaphore.Weighted
	// maxParallelWorkers defines the maximum number of concurrent uploading
	// files. Usually defined by WALG_DOWNLOAD_CONCURRENCY
	maxParallelWorkers int32

	// numUploaded counts the number of files uploaded by BgUploader
	numUploaded int32
	// maxNumUploaded controls the amount of work done in one cycle of
	// archive_command. Usually defined by TOTAL_BG_UPLOADED_LIMIT. This is
	// not enforced exactly. Actual number of files uploaded may be up to
	// maxParallelWorkers higher than this setting.
	maxNumUploaded int32

	// started tracks filenames of ongoing and complete uploads to avoid
	// repeating work
	started map[string]struct{}

	// WAL name where we started.
	firstWalName string
}

// NewBgUploader creates a new BgUploader which looks for WAL files adjacent to
// walFilePath. maxParallelWorkers and maxNumUploaded limits maximum concurrency
// and total work done by this BgUploader respectively.
func NewBgUploader(ctx context.Context, walFilePath string,
	maxParallelWorkers int32,
	maxNumUploaded int32,
	uploader *WalUploader,
	preventWalOverwrite bool,
	readyRename bool) *BgUploader {
	started := make(map[string]struct{})
	firstWalName := filepath.Base(walFilePath)
	started[firstWalName+readySuffix] = struct{}{}
	uploadCtx := ctx
	ctx, cancelFunc := context.WithCancel(ctx)
	return &BgUploader{
		dir:                 filepath.Dir(walFilePath),
		uploader:            uploader,
		preventWalOverwrite: preventWalOverwrite,
		readyRename:         readyRename,

		uploadCtx:          uploadCtx,
		ctx:                ctx,
		cancelFunc:         cancelFunc,
		workerCountSem:     semaphore.NewWeighted(int64(maxParallelWorkers)),
		maxParallelWorkers: maxParallelWorkers,
		numUploaded:        0,
		maxNumUploaded:     maxNumUploaded,
		started:            started,
		firstWalName:       firstWalName,
	}
}

// Start up checking what's inside archive_status
func (b *BgUploader) Start() {
	// Exit early if BgUploader is effectively disabled
	if b.maxParallelWorkers < 1 || b.maxNumUploaded < 1 {
		return
	}

	go b.scanAndProcessFiles()
}

// Stop pipeline. Stop can be safely called concurrently and repeatedly.
func (b *BgUploader) Stop() error {
	// Stop scanning for and enqueueing new uploads. In-flight uploads run on
	// uploadCtx, not b.ctx, so they keep going and drain below.
	b.cancelFunc()
	// Wait for all running uploads. b.ctx is canceled above, so drain with an
	// uncanceled context else Acquire returns immediately instead of waiting.
	return b.workerCountSem.Acquire(context.Background(), int64(b.maxParallelWorkers))
}

// scanAndProcessFiles scans directory for WAL segments and attempts to upload them. It
// makes best effort attempts to avoid duplicating work (re-uploading files).
func (b *BgUploader) scanAndProcessFiles() {
	fileChan := make(chan string)
	defer close(fileChan)
	go b.processFiles(fileChan)

	walName := b.firstWalName

	for i := int32(0); i < b.maxNumUploaded; i++ {
		var err error
		walName, err = GetNextWalFilename(walName)
		if err != nil {
			break
		}
		stat, err := os.Stat(filepath.Join(b.dir, archiveStatusDir, walName+readySuffix))
		if err != nil {
			break
		}
		select {
		case <-b.ctx.Done():
			return
		case fileChan <- stat.Name():
		}
	}

	go b.cleanStatusFiles()

	for {
		files, err := os.ReadDir(filepath.Join(b.dir, archiveStatusDir))
		if err != nil {
			tracelog.ErrorLogger.Print("Error of parallel upload: ", err)
			return
		}

		for _, f := range files {
			select {
			case <-b.ctx.Done():
				return
			case fileChan <- f.Name():
			}
		}

		// Sleep 5 seconds before scanning filesystem again. Exit if
		// BgUploader.Stop() has been invoked.
		select {
		case <-b.ctx.Done():
			return
		case <-time.After(pollPauseDuration):
		}
	}
}

// processFiles reads from input channel and uploads relevant WAL files. Exits
// when the input channel is closed. processFiles also tracks number of
// successfully uploaded WAL files and signals to BgUploader when total count
// has exceeded maxNumUploaded. Concurrency is controlled by semaphore in
// BgUploader.
//
// This function should only be invoked once (in scanFiles)
func (b *BgUploader) processFiles(fileChan <-chan string) {
	var numUploaded atomic.Int32
	for {
		name, ok := <-fileChan
		if !ok {
			break
		}

		if b.shouldSkipFile(name) {
			continue
		}
		if _, ok := b.started[name]; ok {
			continue
		}

		b.started[name] = struct{}{}
		if err := b.workerCountSem.Acquire(b.ctx, 1); err == nil {
			go func() {
				uploadedFile := b.upload(b.uploadCtx, name)
				b.workerCountSem.Release(1)
				if uploadedFile {
					if numUploaded.Add(1) >= b.maxNumUploaded {
						b.cancelFunc()
					}
				}
			}()
		}
	}
}

// shouldSkipFile returns true when the file in question has either already been
// uploaded or if the filename doesn't match the expected pattern
func (b *BgUploader) shouldSkipFile(filename string) bool {
	return !strings.HasSuffix(filename, readySuffix) || b.uploader.ArchiveStatusManager.IsWalAlreadyUploaded(filename)
}

// upload one WAL file. Returns true if the file was uploaded and false if the
// upload failed.
func (b *BgUploader) upload(ctx context.Context, walStatusFilename string) bool {
	walFilename := strings.TrimSuffix(walStatusFilename, readySuffix)
	err := uploadWALFile(ctx, b.uploader.clone(), filepath.Join(b.dir, walFilename), b.preventWalOverwrite)
	if err != nil {
		tracelog.ErrorLogger.Print("Error of background uploader: ", err)
		return false
	}

	err = b.uploader.ArchiveStatusManager.MarkWalUploaded(walFilename)
	if err != nil {
		tracelog.ErrorLogger.Printf("Error marking wal file %s as uploaded: %v", walFilename, err)
	}

	// rename WAL status file ".ready" to ".done" if requested
	if b.readyRename && err == nil {
		err := b.uploader.PGArchiveStatusManager.RenameReady(walFilename)
		// error here is not a fatal thing, just a bit more work for the next wal-push
		tracelog.ErrorLogger.PrintOnError(err)
	}

	return true
}

// cleanStatusFiles scans wal-g archive status directory to remove obsolete
// status files left between runs. A status file is obsolete when the
// corresponding WAL segment no longer has a .ready or .done entry in
// PostgreSQL's archive_status directory. The number of files processed is
// limited to avoid slowing down wal-push on clusters with a large backlog of
// stale marker files.
func (b *BgUploader) cleanStatusFiles() {
	limit := int((b.maxNumUploaded + b.maxParallelWorkers) * 2)
	files, err := b.uploader.ArchiveStatusManager.ListUploaded(limit)
	if err != nil {
		tracelog.ErrorLogger.Print("Failed to list walg archive status files: ", err)
		return
	}

	for _, walName := range files {
		select {
		case <-b.ctx.Done():
			return
		default:
		}

		readyPath := filepath.Join(b.dir, archiveStatusDir, walName+readySuffix)
		donePath := filepath.Join(b.dir, archiveStatusDir, walName+".done")

		_, readyErr := os.Stat(readyPath)
		_, doneErr := os.Stat(donePath)

		if os.IsNotExist(readyErr) && os.IsNotExist(doneErr) {
			err := b.uploader.ArchiveStatusManager.UnmarkWalFile(walName)
			if err != nil {
				tracelog.ErrorLogger.Printf("Failed to remove obsolete walg archive status file %s: %v", walName, err)
			} else {
				tracelog.InfoLogger.Printf("Removed obsolete walg archive status file: %s", walName)
			}
		}
	}
}
