//go:build !windows

package pin

import (
	"io/fs"
	"syscall"
)

func sameFilesystem(_ string, _ string, sourceInfo, pinInfo fs.FileInfo) bool {
	sourceStat, sourceOK := sourceInfo.Sys().(*syscall.Stat_t)
	pinStat, pinOK := pinInfo.Sys().(*syscall.Stat_t)
	return sourceOK && pinOK && sourceStat.Dev == pinStat.Dev
}
