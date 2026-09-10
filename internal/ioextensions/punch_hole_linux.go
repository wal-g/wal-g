//go:build linux

package ioextensions

import (
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

// FileBlockSize returns the filesystem block size used for hole punching.
func FileBlockSize(f *os.File) (int64, error) {
	var stat unix.Stat_t
	if err := unix.Fstat(int(f.Fd()), &stat); err != nil {
		return 0, err
	}
	return int64(stat.Blksize), nil //nolint:unconvert // Blksize is int32 on linux/arm64, int64 on linux/amd64.
}

func PunchHole(f *os.File, offset int64, size int64) error {
	return syscall.Fallocate(
		int(f.Fd()),
		unix.FALLOC_FL_PUNCH_HOLE|unix.FALLOC_FL_KEEP_SIZE,
		offset,
		size,
	)
}
