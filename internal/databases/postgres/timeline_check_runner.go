package postgres

import (
	"bytes"
	"fmt"
	"io"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

type TimelineCheckDetails struct {
	CurrentTimelineID        uint32 `json:"current_timeline_id"`
	HighestStorageTimelineID uint32 `json:"highest_storage_timeline_id"`
}

func (details TimelineCheckDetails) NewPlainTextReader() (io.Reader, error) {
	var outputBuffer bytes.Buffer

	outputBuffer.WriteString(fmt.Sprintf("Highest timeline found in storage: %d\n",
		details.HighestStorageTimelineID))
	outputBuffer.WriteString(fmt.Sprintf("Current cluster timeline: %d\n",
		details.CurrentTimelineID))

	return &outputBuffer, nil
}

// TimelineCheckRunner is used to verify that the current timeline
// is the highest among the storage timelines
type TimelineCheckRunner struct {
	currentTimeline    uint32
	walFolderFilenames []string
}

func (check TimelineCheckRunner) Name() string {
	return "TimelineCheck"
}

func NewTimelineCheckRunner(walFolderFilenames []string,
	currentSegment WalSegmentDescription) (TimelineCheckRunner, error) {
	return TimelineCheckRunner{currentTimeline: currentSegment.Timeline, walFolderFilenames: walFolderFilenames}, nil
}

func (check TimelineCheckRunner) Run() (WalVerifyCheckResult, error) {
	highestTimeline := tryFindHighestTimelineID(check.walFolderFilenames)
	return newTimelineCheckResult(check.currentTimeline, highestTimeline), nil
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

// TODO: Unit tests
func tryFindHighestTimelineID(filenames []string) (highestTimelineID uint32) {
	for _, name := range filenames {
		fileTimeline, ok := tryParseTimelineID(name)
		if !ok {
			tracelog.WarningLogger.Printf(
				"Could not parse the timeline Id from %s. Skipping...",
				name)
			continue
		}

		if highestTimelineID < fileTimeline {
			highestTimelineID = fileTimeline
		}
	}
	return highestTimelineID
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
	if matchResult == nil || len(matchResult) < 2 {
		return 0, false
	}
	fileTimeline, err = ParseTimelineFromString(matchResult[1])
	return fileTimeline, err == nil
}
