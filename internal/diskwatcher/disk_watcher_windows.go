package diskwatcher

import (
	"path/filepath"
	"syscall"
	"unsafe"

	"github.com/wal-g/tracelog"
)

func (w *DiskWatcher) CheckUpperLimit() {

	kernel32 := syscall.MustLoadDLL("kernel32.dll")
	getDiskFreeSpaceEx := kernel32.MustFindProc("GetDiskFreeSpaceExW")

	var freeBytesAvailableToCaller uint64
	var totalNumberOfBytes uint64
	var totalNumberOfFreeBytes uint64

	// The Windows API expects a UTF16 pointer to the path
	ptr, err := syscall.UTF16PtrFromString(filepath.Clean(w.Path))
	if err != nil {
		tracelog.ErrorLogger.Printf("Tried to get %s path but got error: '%v'\n", w.Path, err)
		return
	}
	_, _, err = getDiskFreeSpaceEx.Call(
		uintptr(unsafe.Pointer(ptr)),
		uintptr(unsafe.Pointer(&freeBytesAvailableToCaller)),
		uintptr(unsafe.Pointer(&totalNumberOfBytes)),
		uintptr(unsafe.Pointer(&totalNumberOfFreeBytes)))

	if err != nil {
		tracelog.ErrorLogger.Printf("Tried to call GetDiskFreeSpaceExW at %s but got error: '%v'\n", w.Path, err)
		return
	}

	if totalNumberOfBytes == 0 {
		tracelog.ErrorLogger.Printf("Total disk size of path %s is zero somehow\n", w.Path)
		return
	}

	tracelog.InfoLogger.Printf("free:%d; total:%d; thresh:%d", totalNumberOfFreeBytes, totalNumberOfBytes, w.Threshold)
	if totalNumberOfFreeBytes*100/totalNumberOfBytes < (100 - uint64(w.Threshold)) {
		w.Signaling <- true
	}

}
