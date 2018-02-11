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

// The state of concurrent WAL upload
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
	tu *TarUploader

	// to control amount of work done in one cycle of archive_comand
	totalUploaded int32

	mutex sync.Mutex
}

// Start up checking what's inside archive_status
func (u *BgUploader) Start(walFilePath string, maxParallelWorkers int32, tu *TarUploader) {
	if maxParallelWorkers < 1 {
		return // Nothing to start
	}
	// prepare state
	u.tu = tu
	u.maxParallelWorkers = maxParallelWorkers
	u.dir = filepath.Dir(walFilePath)
	u.started = make(map[string]interface{})
	u.started[filepath.Base(walFilePath)+readySuffix] = walFilePath

	// This goroutine will spawn new if necessary
	go u.CheckForNewFiles()
}

func (u *BgUploader) Stop() {
	for atomic.LoadInt32(&u.parallelWorkers) != 0 {
		time.Sleep(50 * time.Millisecond)
	} // Wait until noone works

	u.mutex.Lock()
	defer u.mutex.Unlock()
	atomic.StoreInt32(&u.maxParallelWorkers, 0) // stop new jobs
	u.running.Wait()                            // wait again for those how jumped to the closing door
}

var readySuffix = ".ready"
var archive_status = "archive_status"
var done = ".done"

// This function could be better represented by the state machine of different wait types
func (u *BgUploader) CheckForNewFiles() {
	for // loop in case if this is last running goroutine
	{
		for haveNoSlots(u) && shouldKeepScanning(u) {
			time.Sleep(10 * time.Millisecond)
		}

		files, err := ioutil.ReadDir(filepath.Join(u.dir, archive_status))

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

			u.mutex.Lock()
			if shouldKeepScanning(u) {
				u.running.Add(1)
				atomic.AddInt32(&u.parallelWorkers, 1)
				go u.Upload(f)
			}
			u.mutex.Unlock()
		}

		if !shouldKeepScanning(u) {
			return
		}

		// If we have slots but no files in FS, we wait for 1 seconds, checking whether we should be interrupted
		// This is done to prevent frequent FS scan
		if !haveNoSlots(u) {
			for i := 0; i < 100; i++ {
				if shouldKeepScanning(u) {
					time.Sleep(20 * time.Millisecond)
				} else {
					return
				}
			}
		}
	}
}

func shouldKeepScanning(u *BgUploader) bool {
	return atomic.LoadInt32(&u.maxParallelWorkers) > 0 && atomic.LoadInt32(&u.totalUploaded) < 1024
}

func haveNoSlots(u *BgUploader) bool {
	return atomic.LoadInt32(&u.parallelWorkers) >= atomic.LoadInt32(&u.maxParallelWorkers)
}

func (u *BgUploader) Upload(info os.FileInfo) {

	walfilename := strings.TrimSuffix(info.Name(), readySuffix)
	UploadWALFile(u.tu, filepath.Join(u.dir, walfilename))

	ready := filepath.Join(u.dir, archive_status, info.Name())
	done := filepath.Join(u.dir, archive_status, walfilename+done)
	err := os.Rename(ready, done)
	if err != nil {
		log.Print("Error renaming .ready to .done: ", err)
	}

	atomic.AddInt32(&u.totalUploaded, 1)

	atomic.AddInt32(&u.parallelWorkers, -1)
	u.running.Done()
}
