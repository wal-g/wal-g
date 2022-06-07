package binary

import (
	"os"
	"time"

	"github.com/wal-g/wal-g/internal/databases/mongo/models"
)

type MongodMeta struct {
	Version     string `json:"Version"`
	ReplSetName string `json:"ReplSetName"`

	StartTS models.Timestamp `json:"TsStart"`
	EndTS   models.Timestamp `json:"TsEnd"`
}

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

type MongodBackupMeta struct {
	//todo: use `internal.GenericMetadata` as base
	BackupName string `json:"BackupName,omitempty"`
	BackupType string `json:"BackupType,omitempty"`

	Hostname   string     `json:"Hostname,omitempty"`
	MongodMeta MongodMeta `json:"MongoMeta,omitempty"`

	StartLocalTime  time.Time `json:"StartLocalTime,omitempty"`
	FinishLocalTime time.Time `json:"FinishLocalTime,omitempty"`

	UncompressedDataSize int64 `json:"UncompressedDataSize,omitempty"`
	CompressedDataSize   int64 `json:"CompressedDataSize,omitempty"`
	Permanent            bool  `json:"Permanent"`

	BackupDirectories []*BackupDirectoryMeta `json:"BackupDirectories,omitempty"`
	BackupFiles       []*BackupFileMeta      `json:"BackupFiles,omitempty"`

	UserData interface{} `json:"UserData,omitempty"`
}
