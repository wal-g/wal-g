package walg

import (
	"time"
)

// BackupTime is used to sort backups by
// latest modified time.
type BackupTime struct {
	Name string
	Time time.Time
}

// TimeSlice represents a backup and its
// last modified time.
type TimeSlice []BackupTime

func (p TimeSlice) Len() int {
	return len(p)
}

func (p TimeSlice) Less(i, j int) bool {
	return p[i].Time.After(p[j].Time)
}

func (p TimeSlice) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
