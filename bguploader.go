package walg

import (
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const TotalBgUploadedLimit = 1024

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

	verify bool
}

// Start up checking what's inside archive_status
func (bgUploader *BgUploader) Start(walFilePath string, maxParallelWorkers int32, uploader *Uploader, verify bool) {
	if maxParallelWorkers < 1 {
		return // Nothing to start
	}
	// prepare state
	bgUploader.uploader = uploader
	bgUploader.maxParallelWorkers = maxParallelWorkers
	bgUploader.dir = filepath.Dir(walFilePath)
	bgUploader.started = make(map[string]interface{})
	bgUploader.started[filepath.Base(walFilePath)+readySuffix] = walFilePath
	bgUploader.verify = verify

	// This goroutine will spawn new if necessary
	go bgUploader.scanOnce()
}

// Stop pipeline
func (bgUploader *BgUploader) Stop() {
	for atomic.LoadInt32(&bgUploader.parallelWorkers) != 0 {
		time.Sleep(50 * time.Millisecond)
	} // Wait until no one works

	bgUploader.mutex.Lock()
	defer bgUploader.mutex.Unlock()
	atomic.StoreInt32(&bgUploader.maxParallelWorkers, 0) // stop new jobs
	bgUploader.running.Wait()                            // wait again for those who jumped to the closing door
}

var readySuffix = ".ready"
var archiveStatus = "archive_status"
var done = ".done"

// TODO : unit tests
func (bgUploader *BgUploader) scanOnce() {
	bgUploader.mutex.Lock()
	defer bgUploader.mutex.Unlock()

	files, err := ioutil.ReadDir(filepath.Join(bgUploader.dir, archiveStatus))
	if err != nil {
		log.Print("Error of parallel upload: ", err)
		return
	}

	for _, f := range files {
		if bgUploader.haveNoSlots() {
			break
		}
		name := f.Name()
		if !strings.HasSuffix(name, readySuffix) {
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
	return atomic.LoadInt32(&bgUploader.maxParallelWorkers) > 0 && atomic.LoadInt32(&bgUploader.totalUploaded) < TotalBgUploadedLimit
}

func (bgUploader *BgUploader) haveNoSlots() bool {
	return atomic.LoadInt32(&bgUploader.parallelWorkers) >= atomic.LoadInt32(&bgUploader.maxParallelWorkers)
}

// TODO : unit tests
// upload one WAL file
func (bgUploader *BgUploader) upload(info os.FileInfo) {
	walFilename := strings.TrimSuffix(info.Name(), readySuffix)
	err := uploadWALFile(bgUploader.uploader.Clone(), filepath.Join(bgUploader.dir, walFilename), bgUploader.verify)
	if err != nil {
		log.Print("Error of background uploader: ", err)
		return
	}

	ready := filepath.Join(bgUploader.dir, archiveStatus, info.Name())
	done := filepath.Join(bgUploader.dir, archiveStatus, walFilename+done)
	err = os.Rename(ready, done)
	if err != nil {
		log.Print("Error renaming .ready to .done: ", err)
	}

	atomic.AddInt32(&bgUploader.totalUploaded, 1)

	bgUploader.scanOnce()
	atomic.AddInt32(&bgUploader.parallelWorkers, -1)

	bgUploader.running.Done()
}
