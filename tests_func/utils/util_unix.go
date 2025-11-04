//go:build !windows
// +build !windows

package utils

import (
	"fmt"
	"os"
)

func setOwner(fileInfo os.FileInfo, sourcePath string, destPath string) error {
	stat, ok := fileInfo.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("failed to get raw syscall.Stat_t data for '%s'", sourcePath)
	}
	if err := os.Lchown(destPath, int(stat.Uid), int(stat.Gid)); err != nil {
		return err
	}
	return nil
}
