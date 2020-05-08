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

// BgUploader represents the state of concurrent WAL upload
type BgUploader struct {
	// pg_[wals|xlog]
	dir string

	// uploading structure
	uploader *WalUploader

	preventWalOverwrite bool

	// ctx signals internals to keep/stop enqueueing more uploads
	ctx context.Context
	// cancelFunc signals internals to stop enqueuing more uploads
	cancelFunc context.CancelFunc

	// workerCount tracks number of concurrent uploaders. Limited to maxParallelWorkers
	workerCount *semaphore.Weighted
	// maxParallelWorkers usually defined by WALG_DOWNLOAD_CONCURRENCY
	maxParallelWorkers int32

	// numUploaded is tracked to control amount of work done in one cycle of archive_command
	numUploaded int32
	// maxNumUploaded controls the amount of work done in one cycle of archive_command
	maxNumUploaded int32

	// started tracks filenames of ongoing and complete uploads to avoid repeating work
	started map[string]struct{}
}

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
		workerCount:        semaphore.NewWeighted(int64(maxParallelWorkers)),
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

	go b.scanFiles()
}

// Stop pipeline. Stop can be safely called concurrently and repeatedly.
func (b *BgUploader) Stop() {
	// Send signal to stop scanning for and uploading new files
	b.cancelFunc()
	// Wait for all running uploads
	b.workerCount.Acquire(context.TODO(), int64(b.maxParallelWorkers))
}

var readySuffix = ".ready"
var archiveStatus = "archive_status"

func (b *BgUploader) scanFiles() {
	fileChan := make(chan os.FileInfo)
	defer close(fileChan)
	go b.processFiles(fileChan)

	for {
		files, err := ioutil.ReadDir(filepath.Join(b.dir, archiveStatus))
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

		// Sleep 5 seconds before scanning filesystem again. Exit if
		// BgUploader.Stop() has been invoked.
		select {
		case <-b.ctx.Done():
			return
		case <-time.After(5 * time.Second):
		}
	}

}

// processFiles reads from input channel and uploads relevant WAL files. Exits
// when the input channel is closed. processFiles also tracks number of
// successfully uploaded WAL files and signals to BgUploader when total count
// has exceeded maxNumUploaded. Concurrency is controlled by semaphore in
// BgUploader
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
		if err := b.workerCount.Acquire(b.ctx, 1); err == nil {
			go func() {
				uploadedFile := b.upload(name)
				b.workerCount.Release(1)
				if uploadedFile {
					if atomic.AddInt32(&numUploaded, 1) >= b.maxNumUploaded {
						b.cancelFunc()
					}
				}
			}()
		}
	}
}

func (b *BgUploader) shouldSkipFile(filename string) bool {
	return !strings.HasSuffix(filename, readySuffix) || b.uploader.ArchiveStatusManager.isWalAlreadyUploaded(filename)
}

// upload one WAL file. Returns true if the file was uploaded and false if the upload failed.
func (b *BgUploader) upload(walStatusFilename string) bool {
	walFilename := strings.TrimSuffix(walStatusFilename, readySuffix)
	err := uploadWALFile(b.uploader.clone(), filepath.Join(b.dir, walFilename), b.preventWalOverwrite)
	if err != nil {
		tracelog.ErrorLogger.Print("Error of background uploader: ", err)
		return false
	}

	if err := b.uploader.ArchiveStatusManager.markWalUploaded(walFilename); err != nil {
		tracelog.ErrorLogger.Printf("Error mark wal file %s uploader due %v", walFilename, err)
	}

	return true
}
