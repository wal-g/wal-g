package postgres

import (
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
)

func getDeltaMap(reader internal.StorageFolderReader,
	timeline uint32,
	firstUsedLSN,
	firstNotUsedLSN LSN) (PagedFileDeltaMap, error) {
	tracelog.InfoLogger.Printf("Timeline: %d, FirstUsedLsn: %s, FirstNotUsedLsn: %s\n",
		timeline, firstUsedLSN, firstNotUsedLSN)
	tracelog.InfoLogger.Printf("First WAL should participate in building delta map: %s",
		newWalSegmentNo(firstUsedLSN).getFilename(timeline))
	tracelog.InfoLogger.Printf("First WAL shouldn't participate in building delta map: %s",
		newWalSegmentNo(firstNotUsedLSN).getFilename(timeline))
	deltaMap := NewPagedFileDeltaMap()
	firstUsedDeltaNo, firstNotUsedDeltaNo := getDeltaRange(firstUsedLSN, firstNotUsedLSN)
	// Get locations from [firstUsedDeltaNo, lastUsedDeltaNo). We use lastUsedDeltaNo in next step
	err := deltaMap.getLocationsFromDeltas(reader, timeline, firstUsedDeltaNo, firstNotUsedDeltaNo.previous())
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during fetch locations from delta files.\n")
	}

	// Handle last delta file separately for fetch locations and walParser from it
	lastDeltaFile, err := getDeltaFile(reader, firstNotUsedDeltaNo.previous().getFilename(timeline))
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during downloading last delta file.\n")
	}
	deltaMap.AddLocationsToDelta(lastDeltaFile.Locations)

	firstUsedWalSegmentNo, firstNotUsedWalSegmentNo := getWalSegmentRange(firstNotUsedDeltaNo, firstNotUsedLSN)
	// we handle WAL files from [firstUsedWalSegmentNo, lastUsedWalSegmentNo]
	err = deltaMap.getLocationsFromWals(reader, timeline, firstUsedWalSegmentNo,
		firstNotUsedWalSegmentNo, lastDeltaFile.WalParser)
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during fetch locations from wal segments.\n")
	}
	return deltaMap, nil
}

func getDeltaRange(firstUsedLsn, firstNotUsedLsn LSN) (DeltaNo, DeltaNo) {
	firstUsedDeltaNo := newDeltaNoFromLsn(firstUsedLsn)
	firstNotUsedDeltaNo := newDeltaNoFromLsn(firstNotUsedLsn)
	return firstUsedDeltaNo, firstNotUsedDeltaNo
}

func getWalSegmentRange(firstNotUsedDeltaNo DeltaNo, firstNotUsedLsn LSN) (WalSegmentNo, WalSegmentNo) {
	firstUsedWalSegmentNo := firstNotUsedDeltaNo.firstWalSegmentNo()
	lastUsedLsn := firstNotUsedLsn - 1
	lastUsedWalSegmentNo := newWalSegmentNo(lastUsedLsn)
	return firstUsedWalSegmentNo, lastUsedWalSegmentNo.next()
}
