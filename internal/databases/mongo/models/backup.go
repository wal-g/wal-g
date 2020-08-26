package models

import "time"

// Backup represents backup sentinel data
type Backup struct {
	BackupName      string      `json:"BackupName,omitempty"`
	StartLocalTime  time.Time   `json:"StartLocalTime,omitempty"`
	FinishLocalTime time.Time   `json:"FinishLocalTime,omitempty"`
	UserData        interface{} `json:"UserData,omitempty"`
	MongoMeta       MongoMeta   `json:"MongoMeta,omitempty"`
	Permanent       bool        `json:"Permanent"`
	DataSize        int64       `json:"DataSize,omitempty"`
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
}

// BackupMeta includes mongodb and storage metadata
type BackupMeta struct {
	Mongo     MongoMeta
	DataSize  int64
	Permanent bool
	User      interface{}
}

// FirstOverlappingBackupForArch checks if archive overlaps any backup from given list.
// TODO: build btree to fix ugly complexity here
func FirstOverlappingBackupForArch(arch Archive, backups []Backup) Backup {
	var backup Backup
	for j := range backups {
		backup = backups[j]
		if ArchInBackup(arch, backup) {
			return backup
		}
	}
	return Backup{}
}

// ArchInBackup checks if archive and given backup overlaps each over.
func ArchInBackup(arch Archive, backup Backup) bool {
	backupStart := backup.MongoMeta.Before.LastMajTS
	backupEnd := backup.MongoMeta.After.LastMajTS
	return TimestampInInterval(arch.Start, backupStart, backupEnd) || TimestampInInterval(arch.End, backupStart, backupEnd) ||
		TimestampInInterval(backupStart, arch.Start, arch.End) || TimestampInInterval(backupEnd, arch.Start, arch.End)
}

// TimestampInInterval checks if timestamp is in given interval.
func TimestampInInterval(ts, begin, end Timestamp) bool {
	return (LessTS(begin, ts) && LessTS(ts, end)) || ts == begin || ts == end
}
