//go:build !windows
// +build !windows

package cache

import (
	"os"

	"golang.org/x/sys/unix"
)

func lockFile(file *os.File, exclusive bool) (err error) {
	how := unix.LOCK_SH
	if exclusive {
		how = unix.LOCK_EX
	}

	for {
		err = unix.Flock(int(file.Fd()), how)
		// When calling syscalls directly, we need to retry EINTR errors. They mean the call was interrupted by a signal.
		if err != unix.EINTR {
			break
		}
	}
	return err
}
