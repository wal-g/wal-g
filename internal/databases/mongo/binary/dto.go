package binary

import (
	"os"
)

type BackupDirectoryMeta struct {
	Path     string      `json:"Path"`
	FileMode os.FileMode `json:"FileMode"`
}

type Checksum struct {
	Algorithm string `json:"Algorithm"`
	Data      string `json:"Data"`
}

type BackupFileMeta struct {
	Path             string      `json:"Path"`
	FileMode         os.FileMode `json:"FileMode"`
	Compression      string      `json:"Compression"`
	CompressedSize   int64       `json:"CompressedSize"`
	UncompressedSize int64       `json:"UncompressedSize"`
	Checksum         Checksum    `json:"Checksum"`
}

type MongodBackupFilesMetadata struct {
	BackupDirectories []*BackupDirectoryMeta `json:"BackupDirectories,omitempty"`
	BackupFiles       []*BackupFileMeta      `json:"BackupFiles,omitempty"`
}
