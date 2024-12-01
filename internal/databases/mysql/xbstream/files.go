package xbstream

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/utility"
)

func safeFileCreate(dataDir string, relFilePath string) (*os.File, error) {
	filePath := filepath.Join(dataDir, relFilePath)
	// FIXME: use os.Root [go 1.24] https://github.com/golang/go/issues/67002
	if !utility.IsInDirectory(filePath, dataDir) {
		return nil, fmt.Errorf("xbstream tries to create file outside destination directory: %v", filePath)
	}

	err := os.MkdirAll(filepath.Dir(filePath), 0777) // FIXME: permissions
	tracelog.ErrorLogger.FatalfOnError("Cannot create new file: %v", err)

	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0666) // FIXME: permissions
	tracelog.ErrorLogger.FatalfOnError("Cannot open new file for write: %v", err)
	return file, nil
}
