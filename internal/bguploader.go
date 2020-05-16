package internal

import (
	"context"
	"io/ioutil"
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

	// ctx signals internals to keep/stop enqueueing more uploads
	ctx context.Context
	// cancelFunc signals internals to stop enqueuing more uploads. It is
	// safe to call this function repeatedly and concurrently.
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
}

// NewBgUploader creates a new BgUploader which looks for WAL files adjacent to
// walFilePath. maxParallelWorkers and maxNumUploaded limits maximum concurrency
// and total work done by this BgUploader respectively.
func NewBgUploader(walFilePath string, maxParallelWorkers int32, maxNumUploaded int32, uploader *WalUploader, preventWalOverwrite bool) *BgUploader {
	started := make(map[string]struct{})
	started[filepath.Base(walFilePath)+readySuffix] = struct{}{}
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &BgUploader{
		dir:                 filepath.Dir(walFilePath),
		uploader:            uploader,
		preventWalOverwrite: preventWalOverwrite,

		ctx:                ctx,
		cancelFunc:         cancelFunc,
		workerCountSem:     semaphore.NewWeighted(int64(maxParallelWorkers)),
		maxParallelWorkers: maxParallelWorkers,
		numUploaded:        0,
		maxNumUploaded:     maxNumUploaded,
		started:            started,
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

// Stop pipeline. Stop can only be called once. Subsequent invokations will deadlock.
func (b *BgUploader) Stop() {
	// Send signal to stop scanning for and uploading new files
	b.cancelFunc()
	// Wait for all running uploads. JANK: grabs all available worker slots
	// and never releases them. This prevents this BgUploader for being
	// restarted (or restopped) in the future.
	b.workerCountSem.Acquire(context.TODO(), int64(b.maxParallelWorkers))
}

// scanAndProcessFiles scans directory for WAL segments and attempts to upload them. It
// makes best effort attempts to avoid duplicating work (re-uploading files).
func (b *BgUploader) scanAndProcessFiles() {
	fileChan := make(chan os.FileInfo)
	defer close(fileChan)
	go b.processFiles(fileChan)

	for {
		files, err := ioutil.ReadDir(filepath.Join(b.dir, archiveStatusDir))
		if err != nil {
			tracelog.ErrorLogger.Print("Error of parallel upload: ", err)
			return
		}

		for _, f := range files {
			select {
			case <-b.ctx.Done():
				return
			case fileChan <- f:
			}
		}

		// Sleep pollPauseDuration before scanning filesystem again.
		// Exit if BgUploader.Stop() has been invoked.
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
func (b *BgUploader) processFiles(fileChan <-chan os.FileInfo) {
	var numUploaded int32 = 0
	for {
		f, ok := <-fileChan
		if !ok {
			break
		}

		name := f.Name()

		if b.shouldSkipFile(name) {
			continue
		}
		if _, ok := b.started[name]; ok {
			continue
		}

		b.started[name] = struct{}{}
		if err := b.workerCountSem.Acquire(b.ctx, 1); err == nil {
			go func() {
				uploadedFile := b.upload(name)
				b.workerCountSem.Release(1)
				if uploadedFile {
					if atomic.AddInt32(&numUploaded, 1) >= b.maxNumUploaded {
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
	return !strings.HasSuffix(filename, readySuffix) || b.uploader.ArchiveStatusManager.isWalAlreadyUploaded(filename)
}

// upload one WAL file. Returns true if the file was uploaded and false if the
// upload failed.
func (b *BgUploader) upload(walStatusFilename string) bool {
	walFilename := strings.TrimSuffix(walStatusFilename, readySuffix)
	err := uploadWALFile(b.uploader.clone(), filepath.Join(b.dir, walFilename), b.preventWalOverwrite)
	if err != nil {
		tracelog.ErrorLogger.Print("Error of background uploader: ", err)
		return false
	}

	if err := b.uploader.ArchiveStatusManager.markWalUploaded(walFilename); err != nil {
		tracelog.ErrorLogger.Printf("Error marking wal file %s as uploaded: %v", walFilename, err)
	}

	return true
}
