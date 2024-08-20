//go:build !linux

package ioextensions

import (
	"os"
	"syscall"
)

func PunchHole(f *os.File, offset int64, size int64) error {
	// do nothing on non-linux platforms
	return syscall.EOPNOTSUPP
}
