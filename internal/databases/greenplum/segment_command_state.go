package greenplum

import (
	"fmt"
	"path"
	"time"

	"github.com/spf13/viper"
	"github.com/wal-g/wal-g/internal"
)

const stateFilesDirPrefix = "walg_seg_states"
const cmdStatePrefix = "cmd_run_state"

func FormatCmdStateName(contentID int, cmdName string) string {
	return fmt.Sprintf("%s_%s_seg%d", cmdStatePrefix, cmdName, contentID)
}

func FormatCmdStatePath(contentID int, cmdName string) string {
	return path.Join(FormatSegmentStateFolderPath(contentID), FormatCmdStateName(contentID, cmdName))
}

func FormatSegmentStateFolderPath(contentID int) string {
	segStatesDirPath := viper.GetString(internal.GPSegmentStatesDir)
	currSegmentStatePath := fmt.Sprintf("%s_seg%d", stateFilesDirPrefix, contentID)
	return path.Join(segStatesDirPath, currSegmentStatePath)
}

type SegCmdStatus string

const (
	RunningCmdStatus     SegCmdStatus = "running"
	FailedCmdStatus      SegCmdStatus = "failed"
	SuccessCmdStatus     SegCmdStatus = "success"
	InterruptedCmdStatus SegCmdStatus = "interrupted"
)

type SegCmdState struct {
	TS     time.Time    `json:"ts"`
	Status SegCmdStatus `json:"status"`
}
