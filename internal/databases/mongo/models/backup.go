package models

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/printlist"
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

func (b *Backup) PrintableFields() []printlist.TableField {
	prettyStartTime := internal.PrettyFormatTime(b.StartLocalTime)
	prettyFinishTime := internal.PrettyFormatTime(b.FinishLocalTime)
	return []printlist.TableField{
		{
			Name:       "name",
			PrettyName: "Name",
			Value:      b.BackupName,
		},
		{
			Name:       "type",
			PrettyName: "Type",
			Value:      b.BackupType,
		},
		{
			Name:       "version",
			PrettyName: "Version",
			Value:      b.MongoMeta.Version,
		},
		{
			Name:        "start_time",
			PrettyName:  "Start time",
			Value:       internal.FormatTime(b.StartLocalTime),
			PrettyValue: &prettyStartTime,
		},
		{
			Name:        "finish_time",
			PrettyName:  "Finish time",
			Value:       internal.FormatTime(b.FinishLocalTime),
			PrettyValue: &prettyFinishTime,
		},
		{
			Name:       "hostname",
			PrettyName: "Hostname",
			Value:      b.Hostname,
		},
		{
			Name:       "start_ts",
			PrettyName: "Start Ts",
			Value:      fmt.Sprintf("%v", b.MongoMeta.Before.LastMajTS.ToBsonTS()),
		},
		{
			Name:       "end_ts",
			PrettyName: "End Ts",
			Value:      fmt.Sprintf("%v", b.MongoMeta.After.LastMajTS.ToBsonTS()),
		},
		{
			Name:       "uncompressed_size",
			PrettyName: "Uncompressed size",
			Value:      strconv.FormatInt(b.UncompressedSize, 10),
		},
		{
			Name:       "compressed_size",
			PrettyName: "Compressed size",
			Value:      strconv.FormatInt(b.CompressedSize, 10),
		},
		{
			Name:       "permanent",
			PrettyName: "Permanent",
			Value:      fmt.Sprintf("%v", b.Permanent),
		},
		{
			Name:       "user_data",
			PrettyName: "User data",
			Value:      marshalUserData(b.UserData),
		},
	}
}

func marshalUserData(userData interface{}) string {
	rawUserData, err := json.Marshal(userData)
	if err != nil {
		rawUserData = []byte(fmt.Sprintf("{\"error\": \"unable to marshal %+v\"}", userData))
	}
	return string(rawUserData)
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
