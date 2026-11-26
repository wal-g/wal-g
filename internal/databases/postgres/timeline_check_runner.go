package postgres

import (
	"bytes"
	"fmt"
	"io"

	"github.com/wal-g/wal-g/utility"
)

type TimelineCheckDetails struct {
	CurrentTimelineID        uint32 `json:"current_timeline_id"`
	HighestStorageTimelineID uint32 `json:"highest_storage_timeline_id"`
}

func (details TimelineCheckDetails) NewPlainTextReader() (io.Reader, error) {
	var outputBuffer bytes.Buffer

	fmt.Fprintf(&outputBuffer, "Highest timeline found in storage: %d\n",
		details.HighestStorageTimelineID)
	fmt.Fprintf(&outputBuffer, "Current cluster timeline: %d\n",
		details.CurrentTimelineID)

	return &outputBuffer, nil
}

// TimelineCheckRunner is used to verify that the current timeline
// is the highest among the storage timelines
type TimelineCheckRunner struct {
	currentTimeline uint32
	highestTimeline uint32
}

func (check TimelineCheckRunner) Name() string {
	return "TimelineCheck"
}

// NewTimelineCheckRunner builds the check from the highest timeline found in storage,
// see walFolderIndex.highestTimeline
func NewTimelineCheckRunner(highestTimeline uint32,
	currentSegment WalSegmentDescription) (TimelineCheckRunner, error) {
	return TimelineCheckRunner{currentTimeline: currentSegment.Timeline, highestTimeline: highestTimeline}, nil
}

func (check TimelineCheckRunner) Run() (WalVerifyCheckResult, error) {
	return newTimelineCheckResult(check.currentTimeline, check.highestTimeline), nil
}

func (check TimelineCheckRunner) Type() WalVerifyCheckType {
	return WalVerifyTimelineCheck
}

// newTimelineCheckResult check produces the WalVerifyCheckResult with status:
// StatusOk if current timeline id matches the highest timeline id found in storage
// StatusWarning if could not determine if current timeline matches the highest in storage
// StatusFailure if current timeline is not equal to the highest timeline id found in storage
func newTimelineCheckResult(currentTimeline, highestTimeline uint32) WalVerifyCheckResult {
	result := WalVerifyCheckResult{
		Status: StatusWarning,
		Details: TimelineCheckDetails{
			CurrentTimelineID:        currentTimeline,
			HighestStorageTimelineID: highestTimeline,
		},
	}
	if highestTimeline > 0 {
		if currentTimeline == highestTimeline {
			result.Status = StatusOk
		} else {
			result.Status = StatusFailure
		}
	}
	return result
}

func tryParseTimelineID(fileName string) (timelineID uint32, success bool) {
	// try to parse timeline id from WAL segment file
	baseName := utility.TrimFileExtension(fileName)
	fileTimeline, _, err := ParseWALFilename(baseName)
	if err == nil {
		return fileTimeline, true
	}

	// try to parse timeline id from .history file
	matchResult := timelineHistoryFileRegexp.FindStringSubmatch(baseName)
	if len(matchResult) < 2 {
		return 0, false
	}
	fileTimeline, err = ParseTimelineFromString(matchResult[1])
	return fileTimeline, err == nil
}
