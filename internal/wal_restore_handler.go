package internal

import (
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/utility"
	"io/ioutil"
	"sort"
)

// Types for sorting
type TimelineSlice []uint32

func (p TimelineSlice) Len() int { return len(p) }

// Because we need sorted slice in descending order
func (p TimelineSlice) Less(i, j int) bool { return p[i] > p[j] }
func (p TimelineSlice) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

type WalSegmentNumbers []WalSegmentNo

func (p WalSegmentNumbers) Len() int { return len(p) }

// Because we need sorted slice in descending order
func (p WalSegmentNumbers) Less(i, j int) bool { return p[i] > p[j] }
func (p WalSegmentNumbers) Swap(i, j int)      { p[i], p[j] = p[j], p[i] }

// HandleWALRestore is invoked to perform wal-g wal-restore
func HandleWALRestore(extFolder storage.Folder, intWalDirectoryPath string) {
	extWalFolder := extFolder.GetSubFolder(utility.WalPath)
	extFolderFilenames, err := getFolderFilenames(extWalFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get the WAL external folder filenames %v\n", err)

	extWalSegments := getSegmentsFromFiles(extFolderFilenames)
	extSegmentsByTimelines := groupSegmentsByTimelines(extWalSegments)

	intFolderFilenames, err := getDirectoryFilenames(intWalDirectoryPath)
	tracelog.ErrorLogger.FatalfOnError("Failed to get the WAL internal folder filenames %v\n", err)

	intWalSegments := getSegmentsFromFiles(intFolderFilenames)
	intSegmentsByTimelines := groupSegmentsByTimelines(intWalSegments)

	filenamesToRestore, err := getFilenamesToRestore(extSegmentsByTimelines, intSegmentsByTimelines)
	tracelog.ErrorLogger.FatalfOnError("Failed to get the needed to restore WAL filenames %v\n", err)

	if len(filenamesToRestore) == 0 {
		tracelog.InfoLogger.Print("No WAL files to restore")
		return
	}

	for _, walFilename := range filenamesToRestore {
		if err = DownloadWALFileTo(extFolder, walFilename, intWalDirectoryPath); err != nil {
			tracelog.ErrorLogger.Printf("Failed to download WAL file %v\n", walFilename)
		} else {
			tracelog.InfoLogger.Printf("Successfully download WAL file %v\n", walFilename)
		}
	}
}

func getFilenamesToRestore(extSegmentsByTl, intSegmentsByTl map[uint32]*WalSegmentsSequence) (filenames []string, err error) {
	extSortTimelines := getSortTimelines(extSegmentsByTl)
	intSortTimelines := getSortTimelines(intSegmentsByTl)

	if extSortTimelines[len(extSortTimelines)-1] < intSortTimelines[len(intSortTimelines)-1] {
		return
	}

	// MaxUint64
	currentSegmentNo := uint64(1<<64 - 1)

	for _, timeline := range extSortTimelines {
		if timeline < intSortTimelines[len(intSortTimelines)-1] {
			return
		}
		segmentNums := getSortWalSegmentNumbers(extSegmentsByTl[timeline].walSegmentNumbers)
		for _, segmentNum := range segmentNums {
			if currentSegmentNo > uint64(segmentNum) {
				continue
			}
			if intSegmentsByTl[timeline] == nil || !intSegmentsByTl[timeline].walSegmentNumbers[segmentNum] {
				filenames = append(filenames, segmentNum.getFilename(timeline))
			}
			currentSegmentNo = uint64(segmentNum)
		}
	}

	return
}

func getSortTimelines(segmentsByTimeline map[uint32]*WalSegmentsSequence) (timelines []uint32) {
	for timeline := range segmentsByTimeline {
		timelines = append(timelines, timeline)
	}
	sort.Sort(TimelineSlice(timelines))
	return
}

func getSortWalSegmentNumbers(walSegmentNumbers map[WalSegmentNo]bool) (result []WalSegmentNo) {
	for walSegmentNo := range walSegmentNumbers {
		result = append(result, walSegmentNo)
	}
	sort.Sort(WalSegmentNumbers(result))
	return
}

func getDirectoryFilenames(directory string) ([]string, error) {
	filesInfo, err := ioutil.ReadDir(directory)
	if err != nil {
		return nil, err
	}

	filenames := make([]string, 0)
	for _, fileInfo := range filesInfo {
		filenames = append(filenames, fileInfo.Name())
	}
	return filenames, nil
}
