package greenplum

import (
	"fmt"
	"path"
	"time"

	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal"
)

const stateFilesDirPrefix = "walg_seg_states"
const backupStatePrefix = "backup_push_state"

func FormatBackupStateName(contentID int, backupName string) string {
	return fmt.Sprintf("%s_%s_seg%d", backupStatePrefix, backupName, contentID)
}

func FormatBackupStatePath(contentID int, backupName string) string {
	return path.Join(FormatSegmentStateFolderPath(contentID), FormatBackupStateName(contentID, backupName))
}

func FormatSegmentStateFolderPath(contentID int) string {
	segStatesDirPath := viper.GetString(internal.GPSegmentStatesDir)
	currSegmentStatePath := fmt.Sprintf("%s_seg%d", stateFilesDirPrefix, contentID)
	return path.Join(segStatesDirPath, currSegmentStatePath)
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
