package internal

import (
	"errors"
	"github.com/spf13/viper"
	"github.com/wal-g/storages/storage"
	"github.com/wal-g/tracelog"
	"os"
	"path/filepath"
)

// HandleWALRestore is invoked to perform wal-g wal-restore
func HandleWALRestore(targetFolder, sourceFolder, cloudFolder storage.Folder) {
	targetPgData, err := ExtractPgControl(targetFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get pg data on target cluster: %v\n", err)

	sourcePgData, err := ExtractPgControl(sourceFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get pg data on source cluster: %v\n", err)

	if targetPgData.GetSystemIdentifier() != sourcePgData.GetSystemIdentifier() {
		tracelog.ErrorLogger.Fatal("System identifiers of target and source clusters are not equal\n")
	}
	if targetPgData.GetCurrentTimeline() == sourcePgData.GetCurrentTimeline() {
		tracelog.ErrorLogger.Fatal("Current timelines of target and source clusters are equal\n")
	}

	tgtHistoryRecords, err := getTimeLineHistoryRecords(targetPgData.GetCurrentTimeline(), targetFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get history data on target cluster: %v\n", err)

	srcHistoryRecords, err := getTimeLineHistoryRecords(sourcePgData.GetCurrentTimeline(), sourceFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get history data on source cluster: %v\n", err)

	lastWalSegmentNo, lastCommonTl, err := FindLastCommonPoint(tgtHistoryRecords, srcHistoryRecords)
	tracelog.ErrorLogger.FatalfOnError("Failed to find last common point: %v\n", err)

	walDir, err := getWalDirName()
	tracelog.ErrorLogger.FatalfOnError("Failed to get WAL directory name: %v\n", err)
	walFolder := sourceFolder.GetSubFolder(walDir)
	folderFilenames, err := getFolderFilenames(walFolder)
	tracelog.ErrorLogger.FatalfOnError("Failed to get WAL filenames: %v\n", err)
	wals := getSegmentsFromFiles(folderFilenames)
	walsByTls := groupSegmentsByTimelines(wals)

	filenamesToRestore, err := GetMissingWals(lastWalSegmentNo, lastCommonTl,
		sourcePgData.GetCurrentTimeline(), historiesSliceToMap(srcHistoryRecords), walsByTls)
	tracelog.ErrorLogger.FatalfOnError("Failed to get missing source WALs: %v\n", err)

	if len(filenamesToRestore) == 0 {
		tracelog.InfoLogger.Print("No WAL files to restore")
		return
	}

	tracelog.ErrorLogger.FatalfOnError("Failed to get the WAL directory name: %v\n", err)
	for _, walFilename := range filenamesToRestore {
		if err = DownloadWALFileTo(cloudFolder, walFilename, sourceFolder.GetSubFolder(walDir).GetPath()); err != nil {
			tracelog.ErrorLogger.Printf("Failed to download WAL file %v\n", walFilename)
		} else {
			tracelog.InfoLogger.Printf("Successfully download WAL file %v\n", walFilename)
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

func uint64Min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

// historiesSliceToMap creates a map where key is timeline and value is lsn where timeline begins
func historiesSliceToMap(slice []*TimelineHistoryRecord) map[uint32]*TimelineHistoryRecord {
	result := make(map[uint32]*TimelineHistoryRecord)
	for _, history := range slice {
		result[history.timeline+1] = history
	}
	result[1] = NewTimelineHistoryRecord(1, 1, "")
	return result
}

// GetMissingWals collect the slice of WAL filenames by last LSN, last timeline,
// current timeline, history records and folder
func GetMissingWals(lastLsn uint64, lastTimeline uint32, currentTimeline uint32,
	historyRecords map[uint32]*TimelineHistoryRecord, walsByTls map[uint32]*WalSegmentsSequence,
) ([]string, error) {
	result := make([]string, 0)

	lsn := uint64(walsByTls[currentTimeline].maxSegmentNo)
	tl := currentTimeline
LOOP:
	for ; tl >= lastTimeline; tl-- {
		walSeq, ok := walsByTls[tl]
		for ; lsn >= historyRecords[tl].lsn; lsn-- {
			if !ok || !walSeq.walSegmentNumbers[WalSegmentNo(lsn)] {
				result = append(result, WalSegmentNo(lsn).getFilename(tl))
			}
			if lsn == lastLsn {
				break LOOP
			}
		}
	}
	if tl != lastTimeline {
		return nil, errors.New("unexpected state: last timeline has no estimated")
	}
	return result, nil
}

func getWalDirName() (string, error) {
	pgData := viper.GetString(PgDataSetting)
	dataFolderPath := filepath.Join(pgData, "pg_wal")
	if _, err := os.Stat(dataFolderPath); err == nil {
		return "pg_wal", nil
	}

	dataFolderPath = filepath.Join(pgData, "pg_xlog")
	if _, err := os.Stat(dataFolderPath); err == nil {
		return "pg_xlog", nil
	}
	return "", errors.New("directory for WAL files doesn't exist in " + pgData + ". Set PGDATA in config correctly")
}
