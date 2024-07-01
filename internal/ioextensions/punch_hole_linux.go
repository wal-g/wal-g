//go:build linux

package ioextensions

import "os"
import "syscall"
import "golang.org/x/sys/unix"

func PunchHole(f *os.File, offset int64, size int64) error {
	return syscall.Fallocate(
		int(f.Fd()),
		unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE,
		offset,
		size,
	)
}
