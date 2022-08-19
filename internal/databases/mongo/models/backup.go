package models

import (
	"time"

	"go.mongodb.org/mongo-driver/bson/primitive"
)

// Backup represents backup sentinel data
// todo: use `internal.GenericMetadata` as base
type Backup struct {
	BackupName       string      `json:"BackupName,omitempty"`
	BackupType       string      `json:"BackupType,omitempty"`
	Hostname         string      `json:"Hostname,omitempty"`
	StartLocalTime   time.Time   `json:"StartLocalTime,omitempty"`
	FinishLocalTime  time.Time   `json:"FinishLocalTime,omitempty"`
	UserData         interface{} `json:"UserData,omitempty"`
	MongoMeta        MongoMeta   `json:"MongoMeta,omitempty"`
	Permanent        bool        `json:"Permanent"`
	UncompressedSize int64       `json:"UncompressedSize,omitempty"`
	CompressedSize   int64       `json:"DataSize,omitempty"`
}

func (b *Backup) Name() string {
	return b.BackupName
}

func (b *Backup) StartTime() time.Time {
	return b.StartLocalTime
}

func (b *Backup) IsPermanent() bool {
	return b.Permanent
}

// NodeMeta represents MongoDB node metadata
type NodeMeta struct {
	LastTS    Timestamp `json:"LastTS,omitempty"`
	LastMajTS Timestamp `json:"LastMajTS,omitempty"`
}

// MongoMeta includes NodeMeta Before and after backup
type MongoMeta struct {
	Before NodeMeta `json:"Before,omitempty"`
	After  NodeMeta `json:"After,omitempty"`

	Version string `json:"Version,omitempty"`

	BackupLastTS primitive.Timestamp `json:"BackupLastTS,omitempty"`
}

// BackupMeta includes mongodb and storage metadata
type BackupMeta struct {
	BackupName     string
	Hostname       string
	Mongo          MongoMeta
	CompressedSize int64
	Permanent      bool
	User           interface{}
	StartTime      time.Time
	FinishTime     time.Time
}

// FirstOverlappingBackupForArch checks if archive overlaps any backup from given list.
// TODO: build btree to fix ugly complexity here
func FirstOverlappingBackupForArch(arch Archive, backups []*Backup) *Backup {
	for _, backup := range backups {
		if ArchInBackup(arch, backup) {
			return backup
		}
	}
	return nil
}

// ArchInBackup checks if archive and given backup overlaps each over.
func ArchInBackup(arch Archive, backup *Backup) bool {
	backupStart := backup.MongoMeta.Before.LastMajTS
	backupEnd := backup.MongoMeta.After.LastMajTS
	return TimestampInInterval(arch.Start, backupStart, backupEnd) ||
		TimestampInInterval(arch.End, backupStart, backupEnd) ||
		TimestampInInterval(backupStart, arch.Start, arch.End) ||
		TimestampInInterval(backupEnd, arch.Start, arch.End)
}

// TimestampInInterval checks if timestamp is in given interval.
func TimestampInInterval(ts, begin, end Timestamp) bool {
	return (LessTS(begin, ts) && LessTS(ts, end)) || ts == begin || ts == end
}
