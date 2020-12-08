package internal

import (
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"sort"
)

// IntegrityCheckRunner queries the current cluster WAL segment and timeline
// and travels through WAL segments in storage in reversed chronological order (starting from that segment)
// to find any missing WAL segments that could potentially fail the PITR procedure
type IntegrityCheckRunner struct {
	startWalSegment           WalSegmentDescription
	stopWalSegmentNo          WalSegmentNo
	uploadingSegmentRangeSize int
	walFolderFilenames        []string
	timelineSwitchMap         map[WalSegmentNo]*TimelineHistoryRecord
}

func NewIntegrityCheckRunner(
	rootFolder storage.Folder,
	walFolderFilenames []string,
	currentWalSegment WalSegmentDescription,
) (IntegrityCheckRunner, error) {
	walFolder := rootFolder.GetSubFolder(utility.WalPath)

	timelineSwitchMap, err := createTimelineSwitchMap(currentWalSegment.Timeline, walFolder)
	if err != nil {
		return IntegrityCheckRunner{}, errors.Wrap(err, "Failed to initialize timeline history map")
	}

	stopWalSegmentNo, err := getEarliestBackupStartSegmentNo(timelineSwitchMap, currentWalSegment.Timeline, rootFolder)
	if err != nil {
		tracelog.WarningLogger.Printf("Failed to detect earliest backup WAL segment no: '%v',"+
			"will scan until the 000000010000000000000001 segment.\n", err)
		stopWalSegmentNo = 1
	}

	// uploadingSegmentRangeSize is needed to determine max amount of missing WAL segments
	// after the last found WAL segment which can be marked as "uploading"
	uploadingSegmentRangeSize, err := getMaxUploadConcurrency()
	if err != nil {
		return IntegrityCheckRunner{}, errors.Wrap(err, "Failed to resolve MaxUploadConcurrency")
	}

	return IntegrityCheckRunner{
		startWalSegment:           currentWalSegment,
		stopWalSegmentNo:          stopWalSegmentNo,
		uploadingSegmentRangeSize: uploadingSegmentRangeSize,
		walFolderFilenames:        walFolderFilenames,
		timelineSwitchMap:         timelineSwitchMap,
	}, nil
}

func (check IntegrityCheckRunner) Run() (WalVerifyCheckResult, error) {
	storageSegments := getSegmentsFromFiles(check.walFolderFilenames)
	walSegmentRunner := NewWalSegmentRunner(check.startWalSegment, storageSegments, check.stopWalSegmentNo, check.timelineSwitchMap)

	segmentScanner := NewWalSegmentScanner(walSegmentRunner)
	err := runWalIntegrityScan(segmentScanner, check.uploadingSegmentRangeSize)
	if err != nil {
		return WalVerifyCheckResult{}, err
	}

	integrityScanSegmentSequences, err := collapseSegmentsByStatusAndTimeline(segmentScanner.ScannedSegments)
	if err != nil {
		return WalVerifyCheckResult{}, err
	}

	return newWalIntegrityCheckResult(integrityScanSegmentSequences), nil
}

func (check IntegrityCheckRunner) Type() WalVerifyCheckType {
	return WalVerifyIntegrityCheck
}

// newWalIntegrityCheckResult check produces the WalVerifyCheckResult with status:
// StatusOk if there are no missing segments in storage
// StatusWarning if storage contains some ProbablyUploading or ProbablyDelayed segments
// StatusFailure if storage contains Lost segments
func newWalIntegrityCheckResult(segmentSequences []*WalIntegrityScanSegmentSequence) WalVerifyCheckResult {
	result := WalVerifyCheckResult{
		Status:  StatusOk,
		Details: segmentSequences,
	}
	for _, row := range segmentSequences {
		switch row.Status {
		case Lost:
			result.Status = StatusFailure
			return result
		case ProbablyDelayed, ProbablyUploading:
			result.Status = StatusWarning
		}
	}
	return result
}

// WalIntegrityScanSegmentSequence is a continuous sequence of segments
// with the same timeline and Status
type WalIntegrityScanSegmentSequence struct {
	TimelineId    uint32               `json:"timeline_id"`
	StartSegment  string               `json:"start_segment"`
	EndSegment    string               `json:"end_segment"`
	SegmentsCount int                  `json:"segments_count"`
	Status        ScannedSegmentStatus `json:"status"`
}

func newWalIntegrityScanSegmentSequence(sequence *WalSegmentsSequence,
	status ScannedSegmentStatus) *WalIntegrityScanSegmentSequence {
	return &WalIntegrityScanSegmentSequence{
		TimelineId:    sequence.timelineId,
		StartSegment:  sequence.minSegmentNo.getFilename(sequence.timelineId),
		EndSegment:    sequence.maxSegmentNo.getFilename(sequence.timelineId),
		Status:        status,
		SegmentsCount: len(sequence.walSegmentNumbers),
	}
}

// runWalIntegrityScan invokes the following storage scan series
// (on each iteration scanner continues from the position where it stopped)
// 1. At first, it runs scan until it finds some segment in WAL storage
// and marks all encountered missing segments as "missing, probably delayed"
// 2. Then it scans exactly uploadingSegmentRangeSize count of segments,
// if found any missing segments it marks them as "missing, probably still uploading"
// 3. Final scan without any limit (until stopSegment is reached),
// all missing segments encountered in this scan are considered as "missing, lost"
func runWalIntegrityScan(scanner *WalSegmentScanner, uploadingSegmentRangeSize int) error {
	// Run to the latest WAL segment available in storage, mark all missing segments as delayed
	err := scanner.Scan(SegmentScanConfig{
		UnlimitedScan:           true,
		StopOnFirstFoundSegment: true,
		MissingSegmentStatus:    ProbablyDelayed,
	})
	if err != nil {
		return err
	}

	// Traverse potentially uploading segments, mark all missing segments as probably uploading
	err = scanner.Scan(SegmentScanConfig{
		ScanSegmentsLimit:    uploadingSegmentRangeSize,
		MissingSegmentStatus: ProbablyUploading,
	})
	if err != nil {
		return err
	}

	// Run until stop segment, and mark all missing segments as lost
	return scanner.Scan(SegmentScanConfig{
		UnlimitedScan:        true,
		MissingSegmentStatus: Lost,
	})
}

// collapseSegmentsByStatusAndTimeline collapses scanned segments
// with the same timeline and Status into segment sequences to minify the output
func collapseSegmentsByStatusAndTimeline(scannedSegments []ScannedSegmentDescription) ([]*WalIntegrityScanSegmentSequence, error) {
	if len(scannedSegments) == 0 {
		return nil, nil
	}

	// make sure that ScannedSegments are ordered
	sort.Slice(scannedSegments, func(i, j int) bool {
		return scannedSegments[i].Number < scannedSegments[j].Number
	})

	segmentSequences := make([]*WalIntegrityScanSegmentSequence, 0)
	currentSequence := NewSegmentsSequence(scannedSegments[0].Timeline, scannedSegments[0].Number)
	currentStatus := scannedSegments[0].status

	for i := 1; i < len(scannedSegments); i++ {
		segment := scannedSegments[i]

		// switch to the new sequence on segment Status change or timeline id change
		if segment.status != currentStatus || currentSequence.timelineId != segment.Timeline {
			segmentSequences = append(segmentSequences, newWalIntegrityScanSegmentSequence(currentSequence, currentStatus))
			currentSequence = NewSegmentsSequence(segment.Timeline, segment.Number)
			currentStatus = segment.status
		} else {
			currentSequence.AddWalSegmentNo(segment.Number)
		}
	}

	segmentSequences = append(segmentSequences, newWalIntegrityScanSegmentSequence(currentSequence, currentStatus))
	return segmentSequences, nil
}

// getEarliestBackupStartSegmentNo returns the starting segmentNo of the earliest available correct backup
func getEarliestBackupStartSegmentNo(timelineSwitchMap map[WalSegmentNo]*TimelineHistoryRecord,
	currentTimeline uint32,
	rootFolder storage.Folder) (WalSegmentNo, error) {
	backups, err := GetBackups(rootFolder)
	if err != nil {
		return 0, err
	}

	backupDetails, err := GetBackupsDetails(rootFolder, backups)
	if err != nil {
		return 0, err
	}

	// switchLsnByTimeline is used for fast lookup of the timeline switch LSN
	switchLsnByTimeline := make(map[uint32]uint64, len(timelineSwitchMap))
	for _, historyRecord := range timelineSwitchMap {
		switchLsnByTimeline[historyRecord.timeline] = historyRecord.lsn
	}
	earliestBackup, err := findEarliestBackup(currentTimeline, backupDetails, switchLsnByTimeline)
	if err != nil {
		return 0, err
	}

	tracelog.InfoLogger.Printf("Detected earliest available backup: %s\n",
		earliestBackup.BackupName)
	return newWalSegmentNo(earliestBackup.StartLsn), nil
}

// findEarliestBackup finds earliest correct backup available in storage.
func findEarliestBackup(
	currentTimeline uint32,
	backupDetails []BackupDetail,
	switchLsnByTimeline map[uint32]uint64,
) (*BackupDetail, error) {
	var earliestBackup *BackupDetail
	for _, backup := range backupDetails {
		backupTimelineId, err := ParseTimelineFromBackupName(backup.BackupName)
		if err != nil {
			return nil, err
		}

		if ok := checkBackupIsCorrect(currentTimeline, backup.BackupName,
			backupTimelineId, backup.StartLsn, switchLsnByTimeline); !ok {
			continue
		}

		if earliestBackup == nil || earliestBackup.StartLsn > backup.StartLsn {
			// create local variable so the reference won't break
			newEarliestBackup := backup
			earliestBackup = &newEarliestBackup
		}
	}
	if earliestBackup == nil {
		return nil, newNoCorrectBackupFoundError()
	}
	return earliestBackup, nil
}

// checkBackupIsCorrect checks that backup start LSN is valid.
// Backup start LSN is considered valid if
// it belongs to range [backup timeline start LSN, backup timeline end LSN]
func checkBackupIsCorrect(
	currentTimeline uint32,
	backupName string,
	backupTimeline uint32,
	backupStartLsn uint64,
	switchLsnByTimeline map[uint32]uint64,
) bool {
	// perform the check only if .history file exists
	if len(switchLsnByTimeline) > 0 {
		// if backup start LSN is less than timeline start LSN => incorrect backup
		if backupTimeline > 1 {
			backupTimelineStartLsn, ok := switchLsnByTimeline[backupTimeline-1]
			if ok && backupStartLsn < backupTimelineStartLsn {
				tracelog.WarningLogger.Printf(
					"checkBackupIsCorrect: %s: backup start LSN %d "+
						"is less than the backup timeline start LSN.\n",
					backupName, backupStartLsn)
				return false
			}
		}

		// if backup belongs to the current timeline, skip the rest of the checks
		if backupTimeline == currentTimeline {
			return true
		}

		// if backup timeline is not present in current .history file => incorrect backup
		timelineSwitchLsn, ok := switchLsnByTimeline[backupTimeline]
		if !ok {
			tracelog.WarningLogger.Printf(
				"checkBackupIsCorrect: %s: backup timeline %d "+
					"is not present in .history file and is not current.\n",
				backupName, backupTimeline)
			return false
		}

		// if backup start LSN is higher than switch LSN of the previous timeline => incorrect backup
		if backupStartLsn >= timelineSwitchLsn {
			tracelog.WarningLogger.Printf(
				"checkBackupIsCorrect: %s: backup start LSN %d "+
					"is higher than the backup timeline end LSN.\n",
				backupName, backupStartLsn)
			return false
		}
	}
	return true
}
