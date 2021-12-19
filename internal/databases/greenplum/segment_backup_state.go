package greenplum

import (
	"fmt"
	"path"
	"time"
)

const stateFilesDirPath = "/tmp/walg_seg_states/"
const backupStatePrefix = "backup_push_state"

func FormatBackupStateName(contentID int, backupName string) string {
	return fmt.Sprintf("%s_%s_seg%d", backupStatePrefix, backupName, contentID)
}

func FormatBackupStatePath(contentID int, backupName string) string {
	return path.Join(stateFilesDirPath, FormatBackupStateName(contentID, backupName))
}

type SegBackupStatus string

const (
	RunningBackupStatus SegBackupStatus = "running"
	FailedBackupStatus  SegBackupStatus = "failed"
	SuccessBackupStatus SegBackupStatus = "success"
)

type SegBackupState struct {
	TS     time.Time       `json:"ts"`
	Status SegBackupStatus `json:"status"`
}
