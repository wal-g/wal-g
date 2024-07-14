//go:build windows
// +build windows

package cache

import (
	"os"
	"syscall"
	"unsafe"
)

var (
	kernel32, _       = syscall.LoadLibrary("kernel32.dll")
	procLockFileEx, _ = syscall.GetProcAddress(kernel32, "LockFileEx")
)

const (
	winLockfileExclusiveLock = 0x00000003
	winLockfileSharedLock    = 0x00000001
)

func lockFile(file *os.File, exclusive bool) (err error) {
	how := winLockfileSharedLock
	if exclusive {
		how = winLockfileExclusiveLock
	}

	_, _, errno := syscall.Syscall6(
		uintptr(procLockFileEx),
		6,
		uintptr(syscall.Handle(file.Fd())),
		uintptr(how),
		uintptr(0), // reserved
		uintptr(1), // lock length (low bytes)  |
		uintptr(0), // lock length (high bytes) |=> entire file
		uintptr(unsafe.Pointer(&syscall.Overlapped{}))) // offset = 0

	err = nil
	if errno != 0 {
		err = errno
	}

	return err
}
