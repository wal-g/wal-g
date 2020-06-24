package internal

import "time"

type BackupFileDescription struct {
	IsIncremented bool // should never be both incremented and Skipped
	IsSkipped     bool
	MTime         time.Time
	UpdatesCount  uint64
}

func NewBackupFileDescription(isIncremented, isSkipped bool, modTime time.Time) *BackupFileDescription {
	return &BackupFileDescription{isIncremented, isSkipped, modTime, 0}
}

type BackupFileList map[string]BackupFileDescription
