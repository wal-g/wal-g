package walg

import (
	"time"
)

/**
 *  Used to sort backup by time
 */
type BackupTime struct {
	Name string
	Time time.Time
}

/**
 *  Used to grab last modified backups on S3.
 */
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
