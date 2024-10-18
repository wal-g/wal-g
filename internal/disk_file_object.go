package internal

import (
	"os"
	"path"
	"time"

	"github.com/pkg/errors"
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

func GetBackupFileMeta(path string) (*BackupFileMeta, error) {
	backupFileInfo, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	return &BackupFileMeta{
		Path:     path,
		FileMode: backupFileInfo.Mode(),
		FileSize: backupFileInfo.Size(),
	}, nil
}

func GetBackupFileMetas(paths []string) ([]*BackupFileMeta, error) {
	var fileMetas []*BackupFileMeta
	for _, path := range paths {
		meta, err := GetBackupFileMeta(path)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to get meta of file %s", path)
		}
		fileMetas = append(fileMetas, meta)
	}
	return fileMetas, nil
}
