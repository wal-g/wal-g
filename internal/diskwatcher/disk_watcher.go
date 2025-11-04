package diskwatcher

import (
	"fmt"
	"os"
	"sync/atomic"
	"time"
)

type DiskWatcher struct {
	Path      string // path to watch
	Threshold int    // percents of disk usage
	Timeout   int    // sleep between checks
	Signaling chan bool
	closed    atomic.Bool
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
		closed:    atomic.Bool{},
	}, nil
}

func (w *DiskWatcher) Start() {
	go func() {
		for {
			if w.closed.Load() {
				return
			}
			w.CheckUpperLimit()
			time.Sleep(time.Duration(w.Timeout) * time.Second)
		}
	}()
}

func (w *DiskWatcher) Stop() {
	close(w.Signaling)
	w.closed.Store(true)
}
