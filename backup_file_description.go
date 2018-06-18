package walg

import "time"

type BackupFileDescription struct {
	IsIncremented bool // should never be both incremented and Skipped
	IsSkipped     bool
	MTime         time.Time
}

type BackupFileList map[string]BackupFileDescription