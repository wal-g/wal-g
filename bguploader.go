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

// BgUploader represents the state of concurrent WAL upload
type BgUploader struct {
	// pg_[wals|xlog]
	dir string

	// count of running gorutines
	parallelWorkers int32

	// usually defined by WALG_DOWNLOAD_CONCURRENCY
	maxParallelWorkers int32

	// waitgroup to handle Stop gracefully
	running sync.WaitGroup

	// every file is attempted only once
	started map[string]interface{}

	// uploading structure
	uploader *Uploader

	// to control amount of work done in one cycle of archive_comand
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
	go scanOnce(bgUploader)
}

// Stop pipeline
func (bgUploader *BgUploader) Stop() {
	for atomic.LoadInt32(&bgUploader.parallelWorkers) != 0 {
		time.Sleep(50 * time.Millisecond)
	} // Wait until noone works

	bgUploader.mutex.Lock()
	defer bgUploader.mutex.Unlock()
	atomic.StoreInt32(&bgUploader.maxParallelWorkers, 0) // stop new jobs
	bgUploader.running.Wait()                            // wait again for those how jumped to the closing door
}

var readySuffix = ".ready"
var archiveStatus = "archive_status"
var done = ".done"

// TODO : unit tests
func scanOnce(u *BgUploader) {
	u.mutex.Lock()
	defer u.mutex.Unlock()

	files, err := ioutil.ReadDir(filepath.Join(u.dir, archiveStatus))
	if err != nil {
		log.Print("Error of parallel upload: ", err)
		return
	}

	for _, f := range files {
		if haveNoSlots(u) {
			break
		}
		name := f.Name()
		if !strings.HasSuffix(name, readySuffix) {
			continue
		}
		if _, ok := u.started[name]; ok {
			continue
		}
		u.started[name] = name

		if shouldKeepScanning(u) {
			u.running.Add(1)
			atomic.AddInt32(&u.parallelWorkers, 1)
			go u.upload(f)
		}
	}
}

func shouldKeepScanning(u *BgUploader) bool {
	return atomic.LoadInt32(&u.maxParallelWorkers) > 0 && atomic.LoadInt32(&u.totalUploaded) < 1024
}

func haveNoSlots(u *BgUploader) bool {
	return atomic.LoadInt32(&u.parallelWorkers) >= atomic.LoadInt32(&u.maxParallelWorkers)
}

// TODO : unit tests
// upload one WAL file
func (bgUploader *BgUploader) upload(info os.FileInfo) {
	walFilename := strings.TrimSuffix(info.Name(), readySuffix)
	uploadWALFile(bgUploader.uploader.Clone(), filepath.Join(bgUploader.dir, walFilename), bgUploader.verify, true)

	ready := filepath.Join(bgUploader.dir, archiveStatus, info.Name())
	done := filepath.Join(bgUploader.dir, archiveStatus, walFilename+done)
	err := os.Rename(ready, done)
	if err != nil {
		log.Print("Error renaming .ready to .done: ", err)
	}

	atomic.AddInt32(&bgUploader.totalUploaded, 1)

	scanOnce(bgUploader)
	atomic.AddInt32(&bgUploader.parallelWorkers, -1)

	bgUploader.running.Done()
}
