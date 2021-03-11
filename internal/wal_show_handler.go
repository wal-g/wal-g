package internal

import (
	"sort"

	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
)

const (
	TimelineOkStatus          = "OK"
	TimelineLostSegmentStatus = "LOST_SEGMENTS"
)

// TimelineInfo contains information about some timeline in storage
type TimelineInfo struct {
	Id               uint32          `json:"id"`
	ParentId         uint32          `json:"parent_id"`
	SwitchPointLsn   uint64          `json:"switch_point_lsn"`
	StartSegment     string          `json:"start_segment"`
	EndSegment       string          `json:"end_segment"`
	SegmentsCount    int             `json:"segments_count"`
	MissingSegments  []string        `json:"missing_segments"`
	Backups          []*BackupDetail `json:"backups,omitempty"`
	SegmentRangeSize uint64          `json:"segment_range_size"`
	Status           string          `json:"status"`
}

func NewTimelineInfo(walSegments *WalSegmentsSequence, historyRecords []*TimelineHistoryRecord) (*TimelineInfo, error) {
	timelineInfo := &TimelineInfo{
		Id:               walSegments.timelineId,
		StartSegment:     walSegments.minSegmentNo.getFilename(walSegments.timelineId),
		EndSegment:       walSegments.maxSegmentNo.getFilename(walSegments.timelineId),
		SegmentsCount:    len(walSegments.walSegmentNumbers),
		SegmentRangeSize: uint64(walSegments.maxSegmentNo-walSegments.minSegmentNo) + 1,
		Status:           TimelineOkStatus,
	}

	missingSegments, err := walSegments.FindMissingSegments()
	if err != nil {
		return nil, err
	}
	timelineInfo.MissingSegments = make([]string, 0, len(missingSegments))
	for _, segment := range missingSegments {
		timelineInfo.MissingSegments = append(timelineInfo.MissingSegments, segment.GetFileName())
	}

	if len(timelineInfo.MissingSegments) > 0 {
		timelineInfo.Status = TimelineLostSegmentStatus
	}

	// set parent timeline id and timeline switch LSN if have .history record available
	if len(historyRecords) > 0 {
		switchHistoryRecord := historyRecords[len(historyRecords)-1]
		timelineInfo.ParentId = switchHistoryRecord.timeline
		timelineInfo.SwitchPointLsn = switchHistoryRecord.lsn
	}
	return timelineInfo, nil
}

// WalSegmentsSequence represents some collection of wal segments with the same timeline
type WalSegmentsSequence struct {
	timelineId        uint32
	walSegmentNumbers map[WalSegmentNo]bool
	minSegmentNo      WalSegmentNo
	maxSegmentNo      WalSegmentNo
}

func NewSegmentsSequence(id uint32, segmentNo WalSegmentNo) *WalSegmentsSequence {
	walSegmentNumbers := make(map[WalSegmentNo]bool)
	walSegmentNumbers[segmentNo] = true

	return &WalSegmentsSequence{
		timelineId:        id,
		walSegmentNumbers: walSegmentNumbers,
		minSegmentNo:      segmentNo,
		maxSegmentNo:      segmentNo,
	}
}

// AddWalSegmentNo adds the provided segment number to collection
func (seq *WalSegmentsSequence) AddWalSegmentNo(number WalSegmentNo) {
	seq.walSegmentNumbers[number] = true
	if seq.minSegmentNo > number {
		seq.minSegmentNo = number
	}
	if seq.maxSegmentNo < number {
		seq.maxSegmentNo = number
	}
}

// FindMissingSegments finds missing segments in range [minSegmentNo, maxSegmentNo]
func (seq *WalSegmentsSequence) FindMissingSegments() ([]WalSegmentDescription, error) {
	startWalSegment := WalSegmentDescription{Number: seq.maxSegmentNo, Timeline: seq.timelineId}

	walSegments := make(map[WalSegmentDescription]bool, len(seq.walSegmentNumbers))
	for number := range seq.walSegmentNumbers {
		segment := WalSegmentDescription{Number: number, Timeline: seq.timelineId}
		walSegments[segment] = true
	}

	// create WAL segment runner to run on single timeline
	walSegmentRunner := NewWalSegmentRunner(startWalSegment, walSegments, seq.minSegmentNo, nil)
	walSegmentScanner := NewWalSegmentScanner(walSegmentRunner)
	err := walSegmentScanner.Scan(SegmentScanConfig{
		UnlimitedScan:        true,
		MissingSegmentStatus: Lost,
	})
	if err != nil {
		return nil, err
	}
	return walSegmentScanner.GetMissingSegmentsDescriptions(), nil
}

// HandleWalShow gets the list of files inside WAL folder, detects the available WAL segments,
// groups WAL segments by the timeline and shows detailed info about each timeline stored in storage
func HandleWalShow(rootFolder storage.Folder, showBackups bool, outputWriter WalShowOutputWriter) {
	walFolder := rootFolder.GetSubFolder(utility.WalPath)
	filenames, err := getFolderFilenames(walFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get the WAL folder filenames %v\n", err)

	walSegments := getSegmentsFromFiles(filenames)
	segmentsByTimelines := groupSegmentsByTimelines(walSegments)

	timelineInfos := make([]*TimelineInfo, 0, len(segmentsByTimelines))
	for _, segmentsSequence := range segmentsByTimelines {
		historyRecords, err := getTimeLineHistoryRecords(segmentsSequence.timelineId, walFolder)
		if err != nil {
			if _, ok := err.(HistoryFileNotFoundError); !ok {
				tracelog.ErrorLogger.Fatalf("Error while loading .history file %v\n", err)
			}
		}

		info, err := NewTimelineInfo(segmentsSequence, historyRecords)
		tracelog.ErrorLogger.FatalfOnError("Error while creating TimeLineInfo %v\n", err)
		timelineInfos = append(timelineInfos, info)
	}

	if showBackups {
		timelineInfos, err = addBackupsInfo(timelineInfos, rootFolder)
		tracelog.ErrorLogger.FatalfOnError("Failed to add backups info: %v\n", err)
	}

	// order timelines by ID
	sort.Slice(timelineInfos, func(i, j int) bool {
		return timelineInfos[i].Id < timelineInfos[j].Id
	})

	err = outputWriter.Write(timelineInfos)
	tracelog.ErrorLogger.FatalfOnError("Error writing output: %v\n", err)
}

func groupSegmentsByTimelines(segments map[WalSegmentDescription]bool) map[uint32]*WalSegmentsSequence {
	segmentsByTimelines := make(map[uint32]*WalSegmentsSequence)
	for segment := range segments {
		if timelineInfo, ok := segmentsByTimelines[segment.Timeline]; ok {
			timelineInfo.AddWalSegmentNo(segment.Number)
			continue
		}
		segmentsByTimelines[segment.Timeline] = NewSegmentsSequence(segment.Timeline, segment.Number)
	}
	return segmentsByTimelines
}

// addBackupsInfo adds info about available backups for each timeline
func addBackupsInfo(timelineInfos []*TimelineInfo, rootFolder storage.Folder) ([]*TimelineInfo, error) {
	backups, err := GetBackups(rootFolder)
	if err != nil {
		return nil, err
	}
	backupDetails, err := GetBackupsDetails(rootFolder, backups)
	if err != nil {
		return nil, err
	}
	for _, info := range timelineInfos {
		info.Backups, err = getBackupsInRange(info.StartSegment, info.EndSegment, info.Id, backupDetails.Data)
		if err != nil {
			return nil, err
		}
	}
	return timelineInfos, nil
}

// getBackupsInRange returns backups taken in range [startSegmentName, endSegmentName]
func getBackupsInRange(startSegmentName, endSegmentName string, timeline uint32,
	backups []BackupDetail) ([]*BackupDetail, error) {
	filteredBackups := make([]*BackupDetail, 0)

	for idx := range backups {
		backup := &backups[idx]
		backupTimeline, _, err := ParseWALFilename(backup.WalFileName)
		if err != nil {
			return nil, err
		}
		startSegment := newWalSegmentNo(backup.StartLsn).getFilename(backupTimeline)
		endSegment := newWalSegmentNo(backup.FinishLsn).getFilename(backupTimeline)

		// there we compare segments lexicographically, maybe it is wrong...
		if timeline == backupTimeline && startSegment >= startSegmentName && endSegment <= endSegmentName {
			filteredBackups = append(filteredBackups, backup)
		}
	}
	return filteredBackups, nil
}
