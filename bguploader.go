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
	tarUploader *TarUploader

	// to control amount of work done in one cycle of archive_comand
	totalUploaded int32

	mutex sync.Mutex

	pre    *S3Prefix
	verify bool
}

// Start up checking what's inside archive_status
func (uploader *BgUploader) Start(walFilePath string, maxParallelWorkers int32, tu *TarUploader, pre *S3Prefix, verify bool) {
	if maxParallelWorkers < 1 {
		return // Nothing to start
	}
	// prepare state
	uploader.tarUploader = tu
	uploader.maxParallelWorkers = maxParallelWorkers
	uploader.dir = filepath.Dir(walFilePath)
	uploader.started = make(map[string]interface{})
	uploader.started[filepath.Base(walFilePath)+readySuffix] = walFilePath
	uploader.pre = pre
	uploader.verify = verify

	// This goroutine will spawn new if necessary
	go scanOnce(uploader)
}

// Stop pipeline
func (uploader *BgUploader) Stop() {
	for atomic.LoadInt32(&uploader.parallelWorkers) != 0 {
		time.Sleep(50 * time.Millisecond)
	} // Wait until noone works

	uploader.mutex.Lock()
	// We have to do this under mutex to exclude interference with shouldKeepScanning() branch
	atomic.StoreInt32(&uploader.maxParallelWorkers, 0) // stop new jobs
	uploader.mutex.Unlock()
	uploader.running.Wait()                            // wait again for those how jumped to the closing door
}

var readySuffix = ".ready"
var archiveStatus = "archive_status"
var done = ".done"

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
			go u.Upload(f)
		}
	}
}

func shouldKeepScanning(u *BgUploader) bool {
	return atomic.LoadInt32(&u.maxParallelWorkers) > 0 && atomic.LoadInt32(&u.totalUploaded) < 1024
}

func haveNoSlots(u *BgUploader) bool {
	return atomic.LoadInt32(&u.parallelWorkers) >= atomic.LoadInt32(&u.maxParallelWorkers)
}

// Upload one WAL file
func (uploader *BgUploader) Upload(info os.FileInfo) {
	walfilename := strings.TrimSuffix(info.Name(), readySuffix)
	UploadWALFile(uploader.tarUploader.Clone(), filepath.Join(uploader.dir, walfilename), uploader.pre, uploader.verify, true)

	ready := filepath.Join(uploader.dir, archiveStatus, info.Name())
	done := filepath.Join(uploader.dir, archiveStatus, walfilename+done)
	err := os.Rename(ready, done)
	if err != nil {
		log.Print("Error renaming .ready to .done: ", err)
	}

	atomic.AddInt32(&uploader.totalUploaded, 1)

	scanOnce(uploader)
	atomic.AddInt32(&uploader.parallelWorkers, -1)

	uploader.running.Done()
}
