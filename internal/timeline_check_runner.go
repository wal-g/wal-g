package internal

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

// TimelineCheckRunner is used to verify that the current timeline
// is the highest among the storage timelines
type TimelineCheckRunner struct {
	currentTimeline  uint32
	storageFileNames []string
}

func (check TimelineCheckRunner) Name() string {
	return "TimelineCheck"
}

func NewTimelineCheckRunner(rootFolder storage.Folder, currentSegment WalSegmentDescription) (TimelineCheckRunner, error) {
	walFolder := rootFolder.GetSubFolder(utility.WalPath)
	storageFileNames, err := getFolderFilenames(walFolder)
	if err != nil {
		return TimelineCheckRunner{}, err
	}
	return TimelineCheckRunner{currentTimeline: currentSegment.Timeline, storageFileNames: storageFileNames}, nil
}

func (check TimelineCheckRunner) Run() (WalVerifyCheckResult, error) {
	highestTimeline := tryFindHighestTimelineId(check.storageFileNames)
	return newTimelineCheckResult(check.currentTimeline, highestTimeline), nil
}

func (check TimelineCheckRunner) Type() WalVerifyCheckType {
	return WalVerifyTimelineCheck
}

type TimelineCheckResult struct {
	CurrentTimelineId        uint32 `json:"current_timeline_id"`
	HighestStorageTimelineId uint32 `json:"highest_storage_timeline_id"`
}

// newTimelineCheckResult check produces the WalVerifyCheckResult with status:
// StatusOk if current timeline id matches the highest timeline id found in storage
// StatusWarning if could not determine if current timeline matches the highest in storage
// StatusFailure if current timeline is not equal to the highest timeline id found in storage
func newTimelineCheckResult(currentTimeline, highestTimeline uint32) WalVerifyCheckResult {
	result := WalVerifyCheckResult{
		Status: StatusWarning,
		Details: TimelineCheckResult{
			CurrentTimelineId:        currentTimeline,
			HighestStorageTimelineId: highestTimeline,
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
func tryFindHighestTimelineId(filenames []string) (highestTimelineId uint32) {
	for _, name := range filenames {
		fileTimeline, ok := tryParseTimelineId(name)
		if !ok {
			tracelog.WarningLogger.Printf(
				"Could not parse the timeline Id from %s. Skipping...",
				name)
			continue
		}

		if highestTimelineId < fileTimeline {
			highestTimelineId = fileTimeline
		}
	}
	return highestTimelineId
}

func tryParseTimelineId(fileName string) (timelineId uint32, success bool) {
	// try to parse timeline id from WAL segment file
	baseName := utility.TrimFileExtension(fileName)
	fileTimeline, _, err := ParseWALFilename(baseName)
	if err == nil {
		return fileTimeline, true
	}

	// try to parse timeline id from .history file
	matchResult := walHistoryFileRegexp.FindStringSubmatch(baseName)
	if matchResult == nil || len(matchResult) < 2 {
		return 0, false
	}
	fileTimeline, err = ParseTimelineFromString(matchResult[1])
	return fileTimeline, err == nil
}
