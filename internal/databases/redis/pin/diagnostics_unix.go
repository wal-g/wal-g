//go:build !windows

package pin

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

func pinAttemptDiagnostics(sourcePath, pinnedPath string) string {
	groups, groupsErr := os.Getgroups()
	return fmt.Sprintf(
		"source=%q source_stat={%s} destination=%q destination_parent_stat={%s} euid=%d egid=%d groups=%v groups_error=%v",
		sourcePath,
		fileDiagnostics(sourcePath),
		pinnedPath,
		fileDiagnostics(filepath.Dir(pinnedPath)),
		os.Geteuid(),
		os.Getegid(),
		groups,
		groupsErr,
	)
}

func fileDiagnostics(path string) string {
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Sprintf("stat_error=%v", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Sprintf("mode=%s size=%d stat_type=%T", info.Mode(), info.Size(), info.Sys())
	}
	return fmt.Sprintf(
		"mode=%s size=%d uid=%d gid=%d device=%d inode=%d links=%d",
		info.Mode(),
		info.Size(),
		stat.Uid,
		stat.Gid,
		stat.Dev,
		stat.Ino,
		stat.Nlink,
	)
}
