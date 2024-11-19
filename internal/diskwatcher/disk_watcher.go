package diskwatcher

import (
	"fmt"
	"os"
	"syscall"
	"time"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/abool"
)

type DiskWatcher struct {
	Path      string // path to watch
	Threshold int    // percents of disk usage
	Timeout   int    // sleep between checks
	Signaling chan bool
	closed    *abool.AtomicBool
}

func NewDiskWatcher(threshold int, path string, timeout int) (*DiskWatcher, error) {
	if path == "" {
		return nil, fmt.Errorf("path for disk watcher should be set, got empty")
	}
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return nil, fmt.Errorf("path for disk watcher does not exists: %s", path)
	}
	return &DiskWatcher{
		Path:      path,
		Threshold: threshold,
		Timeout:   timeout,
		Signaling: make(chan bool, 1),
		closed:    abool.New(),
	}, nil
}

func (w *DiskWatcher) Start() {
	go func() {
		for {
			if w.closed.IsSet() {
				return
			}
			w.CheckUpperLimit()
			time.Sleep(time.Duration(w.Timeout) * time.Second)
		}
	}()
}

func (w *DiskWatcher) Stop() {
	close(w.Signaling)
	w.closed.Set()
}

func (w *DiskWatcher) CheckUpperLimit() {
	fs := syscall.Statfs_t{}
	err := syscall.Statfs(w.Path, &fs)
	if err != nil {
		tracelog.ErrorLogger.Printf("Tried to get %s path sys data, but got error: '%v'\n", w.Path, err)
		return
	}

	total := fs.Blocks
	if total == 0 {
		tracelog.ErrorLogger.Printf("Total disk size of path %s is zero somehow\n", w.Path)
		return
	}

	free := fs.Bfree
	tracelog.InfoLogger.Printf("free:%d; total:%d; thresh:%d", free, total, w.Threshold)
	if free*100/total < (100 - uint64(w.Threshold)) {
		w.Signaling <- true
	}
}
