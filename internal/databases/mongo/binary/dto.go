package binary

import (
	"os"
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

type MongodMeta struct {
	Version      string   `json:"Version"`
	ReplSetNames []string `json:"ReplSetNames"`

	StartTS primitive.Timestamp `json:"TsStart"`
	EndTS   primitive.Timestamp `json:"TsEnd"`

	BackupLastTS primitive.Timestamp `json:"BackupLastTS"`
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

type MongodBackupSentinel struct {
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

	UserData interface{} `json:"UserData,omitempty"`
}

type MongodBackupFilesMetadata struct {
	BackupDirectories []*BackupDirectoryMeta `json:"BackupDirectories,omitempty"`
	BackupFiles       []*BackupFileMeta      `json:"BackupFiles,omitempty"`
}
