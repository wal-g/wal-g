//go:build !linux

package ioextensions

import (
	"os"
	"syscall"
)

// FileBlockSize is unsupported where hole punching is unavailable.
func FileBlockSize(f *os.File) (int64, error) {
	return 0, syscall.EOPNOTSUPP
}

func PunchHole(f *os.File, offset int64, size int64) error {
	// do nothing on non-linux platforms
	return syscall.EOPNOTSUPP
}
