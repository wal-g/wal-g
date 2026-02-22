//go:build !windows
// +build !windows

package diskwatcher

import (
	"fmt"
	"log/slog"
	"syscall"

	"github.com/wal-g/tracelog"
)

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
	slog.Info(fmt.Sprintf("free:%d; total:%d; thresh:%d", free, total, w.Threshold))
	if free*100/total < (100 - uint64(w.Threshold)) {
		w.Signaling <- true
	}
}
