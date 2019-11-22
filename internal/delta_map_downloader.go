package internal

import (
	"github.com/pkg/errors"
	"github.com/wal-g/storages/storage"
	"github.com/tinsane/tracelog"
)

func GetDeltaMap(folder storage.Folder, timeline uint32, firstUsedLSN, firstNotUsedLSN uint64) (PagedFileDeltaMap, error) {
	tracelog.InfoLogger.Printf("Timeline: %d, FirstUsedLsn: %d, FirstNotUsedLsn: %d\n", timeline, firstUsedLSN, firstNotUsedLSN)
	tracelog.InfoLogger.Printf("First WAL should participate in building delta map: %s", NewWalSegmentNo(firstUsedLSN).GetFilename(timeline))
	tracelog.InfoLogger.Printf("First WAL shouldn't participate in building delta map: %s", NewWalSegmentNo(firstNotUsedLSN).GetFilename(timeline))
	deltaMap := NewPagedFileDeltaMap()
	firstUsedDeltaNo, firstNotUsedDeltaNo := GetDeltaRange(firstUsedLSN, firstNotUsedLSN)
	// Get locations from [firstUsedDeltaNo, lastUsedDeltaNo). We use lastUsedDeltaNo in next step
	err := deltaMap.GetLocationsFromDeltas(folder, timeline, firstUsedDeltaNo, firstNotUsedDeltaNo.Previous())
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during fetch locations from delta files.\n")
	}

	// Handle last delta file separately for fetch locations and walParser from it
	lastDeltaFile, err := getDeltaFile(folder, firstNotUsedDeltaNo.Previous().GetFilename(timeline))
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during downloading last delta file.\n")
	}
	deltaMap.AddLocationsToDelta(lastDeltaFile.Locations)

	firstUsedWalSegmentNo, firstNotUsedWalSegmentNo := GetWalSegmentRange(firstNotUsedDeltaNo, firstNotUsedLSN)
	// we handle WAL files from [firstUsedWalSegmentNo, lastUsedWalSegmentNo]
	err = deltaMap.GetLocationsFromWals(folder, timeline, firstUsedWalSegmentNo, firstNotUsedWalSegmentNo, lastDeltaFile.WalParser)
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during fetch locations from wal segments.\n")
	}
	return deltaMap, nil
}

func GetDeltaRange(firstUsedLsn, firstNotUsedLsn uint64) (DeltaNo, DeltaNo) {
	firstUsedDeltaNo := NewDeltaNoFromLsn(firstUsedLsn)
	firstNotUsedDeltaNo := NewDeltaNoFromLsn(firstNotUsedLsn)
	return firstUsedDeltaNo, firstNotUsedDeltaNo
}

func GetWalSegmentRange(firstNotUsedDeltaNo DeltaNo, firstNotUsedLsn uint64) (WalSegmentNo, WalSegmentNo) {
	firstUsedWalSegmentNo := firstNotUsedDeltaNo.FirstWalSegmentNo()
	lastUsedLsn := firstNotUsedLsn - 1
	lastUsedWalSegmentNo := NewWalSegmentNo(lastUsedLsn)
	return firstUsedWalSegmentNo, lastUsedWalSegmentNo.Next()
}
