package postgres

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/pkg/storages/storage"
	"github.com/wal-g/wal-g/utility"
)

type TimelineWithSegmentNo struct {
	timeline  uint32
	segmentNo uint64
}

func NewTimelineWithSegmentNo(tl uint32, seg uint64) *TimelineWithSegmentNo {
	return &TimelineWithSegmentNo{timeline: tl, segmentNo: seg}
}

func NewTimelineWithSegmentNoBy(record *TimelineHistoryRecord) *TimelineWithSegmentNo {
	return NewTimelineWithSegmentNo(record.timeline, getSegmentNoFromLsn(record.lsn))
}

// HandleWALRestore is invoked to perform wal-g wal-restore
func HandleWALRestore(targetPath, sourcePath string, cloudFolder storage.Folder, isTargetRemote bool, requisites SshRequisites) {
	cloudFolder = cloudFolder.GetSubFolder(utility.WalPath)

	var targetPgData *PgControlData
	var err error
	if isTargetRemote {
		targetPgData, err = ExtractRemotePgControl(targetPath, requisites)
		tracelog.ErrorLogger.FatalfOnError("Failed to get pg data on remote target cluster: %s\n", err)
	} else {
		targetPgData, err = ExtractPgControl(targetPath)
		tracelog.ErrorLogger.FatalfOnError("Failed to get pg data on target cluster: %s\n", err)
	}

	sourcePgData, err := ExtractPgControl(sourcePath)
	tracelog.ErrorLogger.FatalfOnError("Failed to get pg data on source cluster: %s\n", err)

	if targetPgData.GetSystemIdentifier() != sourcePgData.GetSystemIdentifier() {
		tracelog.ErrorLogger.Fatal("System identifiers of target and source clusters are not equal\n")
	}
	if targetPgData.GetCurrentTimeline() == sourcePgData.GetCurrentTimeline() {
		tracelog.ErrorLogger.Fatal("Latest checkpoint timelines of target and source clusters are equal\n")
	}

	targetWalDir, err := getWalDirName(targetPath)
	tracelog.ErrorLogger.FatalfOnError("Failed to get WAL directory name: %s\n", err)
	sourceWalDir, err := getWalDirName(sourcePath)
	tracelog.ErrorLogger.FatalfOnError("Failed to get WAL directory name: %s\n", err)

	tgtHistoryRecords, err := getLocalTimelineHistoryRecords(targetPgData.GetCurrentTimeline(), targetWalDir)
	tracelog.ErrorLogger.FatalfOnError("Failed to get history data on target cluster: %s\n", err)
	srcHistoryRecords, err := getLocalTimelineHistoryRecords(sourcePgData.GetCurrentTimeline(), sourceWalDir)
	tracelog.ErrorLogger.FatalfOnError("Failed to get history data on source cluster: %s\n", err)

	lastCommonLsn, lastCommonTl, err := FindLastCommonPoint(tgtHistoryRecords, srcHistoryRecords)
	tracelog.ErrorLogger.FatalfOnError("Failed to find last common point: %s\n", err)

	srcTimelineWithSegNo := transformTimelineHistoryRecords(srcHistoryRecords)
	mapOfSrcTimelineWithSegNo := timelineWithSegmentNoSliceToMap(srcTimelineWithSegNo)

	folderFilenames, err := getDirectoryFilenames(sourceWalDir)
	tracelog.ErrorLogger.FatalfOnError("Failed to get WAL filenames: %s\n", err)

	walsByTimelines := groupSegmentsByTimelines(getSegmentsFromFiles(folderFilenames))

	filenamesToRestore, err := GetMissingWals(
		getSegmentNoFromLsn(lastCommonLsn), lastCommonTl,
		sourcePgData.GetCurrentTimeline(), mapOfSrcTimelineWithSegNo, walsByTimelines)
	tracelog.ErrorLogger.FatalfOnError("Failed to get missing source WALs: %s\n", err)

	if len(filenamesToRestore) == 0 {
		tracelog.InfoLogger.Println("No WAL files to restore")
		return
	}
	tracelog.InfoLogger.Printf("WAL files to restore: %v", filenamesToRestore)
	for _, walFilename := range filenamesToRestore {
		location := utility.ResolveSymlink(path.Join(sourceWalDir, walFilename))
		if err = internal.DownloadFileTo(cloudFolder, walFilename, location); err != nil {
			tracelog.ErrorLogger.Printf("Failed to download WAL file %s: %s\n", walFilename, err)
		} else {
			tracelog.InfoLogger.Printf("Successfully download WAL file %s\n", walFilename)
		}
	}
}

// FindLastCommonPoint get the last common LSN and timeline between two slices of
// history records. Nil input is not handle
func FindLastCommonPoint(target, source []*TimelineHistoryRecord) (uint64, uint32, error) {
	currentLsn := uint64(1)
	currentTimeline := uint32(1)

	if len(target) == len(source) {
		return currentLsn, currentTimeline, errors.New("two clusters on the same timeline")
	}

	if len(target) == 0 {
		currentLsn = source[0].lsn
		currentTimeline = source[0].timeline
	}
	if len(source) == 0 {
		currentLsn = target[0].lsn
		currentTimeline = target[0].timeline
	}
	for i, tgtRecord := range target {
		if len(source) <= i {
			break
		}

		if tgtRecord.lsn == source[i].lsn {
			currentLsn = tgtRecord.lsn
			currentTimeline = tgtRecord.timeline
		} else {
			currentLsn = uint64Min(tgtRecord.lsn, source[i].lsn)
			currentTimeline = tgtRecord.timeline
			break
		}
	}
	return currentLsn, currentTimeline, nil
}

// GetMissingWals collect the slice of WAL filenames by last LSN, last timeline,
// current timeline, history records and folder
func GetMissingWals(lastSeg uint64, lastTl, currentTl uint32,
	tlToSeg map[uint32]*TimelineWithSegmentNo,
	walsByTimelines map[uint32]*WalSegmentsSequence,
) ([]string, error) {
	result := make([]string, 0)
	currentSeg := uint64(walsByTimelines[currentTl].maxSegmentNo)

	for ; currentTl >= lastTl; currentTl-- {
		// Get wal segment sequence for current timeline
		walSegSeq, ok := walsByTimelines[currentTl]

		// Iterate over wal segment sequence for current timeline
		for ; currentSeg >= tlToSeg[currentTl].segmentNo; currentSeg-- {
			// Making sure that this wal segment sequence is correct and check for existing segment
			if !ok || !walSegSeq.walSegmentNumbers[WalSegmentNo(currentSeg)] {
				result = append(result, WalSegmentNo(currentSeg).getFilename(currentTl))
			}

			if currentSeg == lastSeg {
				return result, nil
			}
		}
	}
	return result, nil
}

// getLocalTimelineHistoryRecords gets timeline history records from local wal history files
func getLocalTimelineHistoryRecords(startTimeline uint32, pathToWal string) ([]*TimelineHistoryRecord, error) {
	if startTimeline == 1 {
		return make([]*TimelineHistoryRecord, 0), nil
	}
	historyReadCloser, err := getLocalHistoryFile(startTimeline, pathToWal)
	if err != nil {
		return nil, err
	}
	historyRecords, err := parseHistoryFile(historyReadCloser)
	if err != nil {
		return nil, err
	}
	err = historyReadCloser.Close()
	if err != nil {
		return nil, err
	}
	return historyRecords, nil
}

// transformTimelineHistoryRecords transforms slice of TimelineHistoryRecord to TimelineWithSegmentNo for
// a comfortable iteration over the wal records
func transformTimelineHistoryRecords(records []*TimelineHistoryRecord) []*TimelineWithSegmentNo {
	result := make([]*TimelineWithSegmentNo, 0, len(records))
	for _, record := range records {
		result = append(result, NewTimelineWithSegmentNoBy(record))
	}
	return result
}

// timelineWithSegmentNoSliceToMap creates a map where key is timeline and value is lsn where timeline begins
func timelineWithSegmentNoSliceToMap(slice []*TimelineWithSegmentNo) map[uint32]*TimelineWithSegmentNo {
	result := make(map[uint32]*TimelineWithSegmentNo)
	for _, el := range slice {
		result[el.timeline+1] = el
	}
	result[1] = &TimelineWithSegmentNo{
		timeline:  0,
		segmentNo: 1,
	}
	return result
}

// getDirectoryFilenames returns slice of filenames in directory by path
func getDirectoryFilenames(path string) ([]string, error) {
	result := make([]string, 0)
	files, err := ioutil.ReadDir(path)
	if err != nil {
		return nil, err
	}
	for _, f := range files {
		result = append(result, f.Name())
	}
	return result, nil
}

func getLocalHistoryFile(timeline uint32, pathToWal string) (io.ReadCloser, error) {
	historyFileName := fmt.Sprintf(walHistoryFileFormat, timeline)
	readCloser, err := os.Open(filepath.Join(pathToWal, historyFileName))
	if err != nil {
		return nil, err
	}
	return readCloser, nil
}

func getWalDirName(pgData string) (string, error) {
	dataFolderPath := filepath.Join(pgData, "pg_wal")
	if _, err := os.Stat(dataFolderPath); err == nil {
		return dataFolderPath, nil
	}

	dataFolderPath = filepath.Join(pgData, "pg_xlog")
	if _, err := os.Stat(dataFolderPath); err == nil {
		return dataFolderPath, nil
	}
	return "", errors.New("directory for WAL files doesn't exist in " + pgData)
}

func uint64Min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
