package binary

import (
	"os"
	"path"
	"time"
)

type MongodBackupFilesMetadata struct {
	BackupDirectories []*BackupFileMeta `json:"BackupDirectories,omitempty"` // used only for restoring old backups
	BackupFiles       []*BackupFileMeta `json:"BackupFiles,omitempty"`
}

type BackupFileMeta struct {
	Path        string      `json:"Path"`
	FileMode    os.FileMode `json:"FileMode"`
	Compression string      `json:"Compression"` // used only for restoring old backups
	FileSize    int64       `json:"-"`
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
