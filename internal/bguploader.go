package internal

import (
	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal/tracelog"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// BgUploader represents the state of concurrent WAL upload
type BgUploader struct {
	// pg_[wals|xlog]
	dir string

	// count of running goroutines
	parallelWorkers int32

	// usually defined by WALG_DOWNLOAD_CONCURRENCY
	maxParallelWorkers int32

	// waitgroup to handle Stop gracefully
	running sync.WaitGroup

	// every file is attempted only once
	started map[string]interface{}

	// uploading structure
	uploader *Uploader

	// to control amount of work done in one cycle of archive_command
	totalUploaded int32

	mutex sync.Mutex

	preventWalOverwrite bool
}

func NewBgUploader(walFilePath string, maxParallelWorkers int32, uploader *Uploader, preventWalOverwrite bool) *BgUploader {
	started := make(map[string]interface{})
	started[filepath.Base(walFilePath)+readySuffix] = walFilePath
	return &BgUploader{
		filepath.Dir(walFilePath),
		0,
		maxParallelWorkers,
		sync.WaitGroup{},
		started,
		uploader,
		0,
		sync.Mutex{},
		preventWalOverwrite,
	}
}

// Start up checking what's inside archive_status
func (bgUploader *BgUploader) Start() {
	if bgUploader.maxParallelWorkers < 1 {
		return // Nothing to start
	}
	// This goroutine will spawn new if necessary
	go bgUploader.scanOnce()
}

// Stop pipeline
func (bgUploader *BgUploader) Stop() {
	for atomic.LoadInt32(&bgUploader.parallelWorkers) != 0 {
		time.Sleep(50 * time.Millisecond)
	} // Wait until no one works

	bgUploader.mutex.Lock()
	// We have to do this under mutex to exclude interference with shouldKeepScanning() branch
	atomic.StoreInt32(&bgUploader.maxParallelWorkers, 0) // stop new jobs
	bgUploader.mutex.Unlock()

	bgUploader.running.Wait() // wait again for those who jumped to the closing door
}

var readySuffix = ".ready"
var archiveStatus = "archive_status"

// TODO : unit tests
func (bgUploader *BgUploader) scanOnce() {
	bgUploader.mutex.Lock()
	defer bgUploader.mutex.Unlock()

	files, err := ioutil.ReadDir(filepath.Join(bgUploader.dir, archiveStatus))
	if err != nil {
		tracelog.ErrorLogger.Print("Error of parallel upload: ", err)
		return
	}

	for _, f := range files {
		if bgUploader.haveNoSlots() {
			break
		}
		name := f.Name()
		if isWalAlreadyUploaded(bgUploader.uploader, name) {
			continue
		}
		if _, ok := bgUploader.started[name]; ok {
			continue
		}
		bgUploader.started[name] = name

		if bgUploader.shouldKeepScanning() {
			bgUploader.running.Add(1)
			atomic.AddInt32(&bgUploader.parallelWorkers, 1)
			go bgUploader.upload(f)
		}
	}
}

func (bgUploader *BgUploader) shouldKeepScanning() bool {
	return atomic.LoadInt32(&bgUploader.maxParallelWorkers) > 0 && atomic.LoadInt32(&bgUploader.totalUploaded) < viper.GetInt32(TotalBgUploadedLimit)
}

func (bgUploader *BgUploader) haveNoSlots() bool {
	return atomic.LoadInt32(&bgUploader.parallelWorkers) >= atomic.LoadInt32(&bgUploader.maxParallelWorkers)
}

// TODO : unit tests
// upload one WAL file
func (bgUploader *BgUploader) upload(info os.FileInfo) {
	actualName := info.Name()
	walFilename := strings.TrimSuffix(actualName, filepath.Ext(actualName))
	err := UploadWALFile(bgUploader.uploader.Clone(), filepath.Join(bgUploader.dir, walFilename), bgUploader.preventWalOverwrite)
	if err != nil {
		tracelog.ErrorLogger.Print("Error of background uploader: ", err)
		return
	}

	if err := markWalUploaded(bgUploader.uploader, walFilename); err != nil {
		tracelog.ErrorLogger.Printf("Error mark wal file %s uploader due %v", walFilename, err)
	}

	atomic.AddInt32(&bgUploader.totalUploaded, 1)

	bgUploader.scanOnce()
	atomic.AddInt32(&bgUploader.parallelWorkers, -1)

	bgUploader.running.Done()
}
