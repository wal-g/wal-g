package binary

import (
	"os"
	"path"
	"time"
)

type BackupFileMeta struct {
	Path     string
	FileMode os.FileMode
	FileSize int64
}

func (backupFileMeta *BackupFileMeta) Name() string {
	return path.Base(backupFileMeta.Path)
}

func (backupFileMeta *BackupFileMeta) Size() int64 {
	return backupFileMeta.FileSize
}

func (backupFileMeta *BackupFileMeta) Mode() os.FileMode {
	return backupFileMeta.FileMode
}

func (backupFileMeta *BackupFileMeta) ModTime() time.Time {
	return time.Now()
}

func (backupFileMeta *BackupFileMeta) IsDir() bool {
	return backupFileMeta.FileMode.IsDir()
}

func (backupFileMeta *BackupFileMeta) Sys() any {
	return nil
}
