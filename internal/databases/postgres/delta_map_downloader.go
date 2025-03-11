package postgres

import (
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal"
	"github.com/wal-g/wal-g/internal/walparser"
)

func getDeltaMap(reader internal.StorageFolderReader,
	timeline uint32,
	firstUsedLSN,
	firstNotUsedLSN LSN) (PagedFileDeltaMap, error) {
	firstUsedWalSegmentNo := NewWalSegmentNo(firstUsedLSN)
	firstNotUsedWalSegmentNo := NewWalSegmentNo(firstNotUsedLSN)
	tracelog.InfoLogger.Printf("Timeline: %d, FirstUsedLsn: %s, FirstNotUsedLsn: %s\n",
		timeline, firstUsedLSN, firstNotUsedLSN)
	tracelog.InfoLogger.Printf("First WAL should participate in building delta map: %s",
		firstUsedWalSegmentNo.GetFilename(timeline))
	tracelog.InfoLogger.Printf("First WAL shouldn't participate in building delta map: %s",
		firstNotUsedWalSegmentNo.GetFilename(timeline))
	deltaMap := NewPagedFileDeltaMap()
	firstUsedDeltaNo, firstNotUsedDeltaNo := getDeltaRange(firstUsedLSN, firstNotUsedLSN)
	tracelog.InfoLogger.Printf("First DELTA should participate in building delta map: %s",
		firstUsedDeltaNo.getFilename(timeline))
	tracelog.InfoLogger.Printf("First DELTA shouldn't participate in building delta map: %s",
		firstNotUsedDeltaNo.getFilename(timeline))
	firstDeltaFile, err := getDeltaFile(reader, firstUsedDeltaNo.getFilename(timeline))
	var walparser *walparser.WalParser = nil
	/* First delta file not found , parse it from wal log
	 * If the first wal file pushed to the wal-g is not started with xxx0, and the basebackup
	 * started, then the first delta file will always be missing for that basebackup. For this
	 * case, we will parse the delta map from wals for the first delta segment
	 */
	if err != nil {
		err, walparser = handlFirstDeltaFileMiss(firstUsedDeltaNo, firstUsedWalSegmentNo, firstNotUsedWalSegmentNo, timeline, reader, deltaMap)
		if err != nil {
			return deltaMap, errors.Wrapf(err, "Error during downloading first delta file.\n")
		}
	} else {
		deltaMap.AddLocationsToDelta(firstDeltaFile.Locations)
		walparser = firstDeltaFile.WalParser
	}
	if firstUsedDeltaNo == firstNotUsedDeltaNo {
		return deltaMap, nil
	}

	/* The last delta segment is the same as the first delta segment */
	if firstUsedDeltaNo == firstNotUsedDeltaNo.previous() {
		err = deltaMap.getLocationsFromWals(reader, timeline, firstNotUsedDeltaNo.firstWalSegmentNo(),
			firstNotUsedWalSegmentNo, walparser)
		return deltaMap, err
	}
	// Get locations from (firstUsedDeltaNo, lastUsedDeltaNo). We use lastUsedDeltaNo in next step
	err = deltaMap.getLocationsFromDeltas(reader, timeline, firstUsedDeltaNo.next(), firstNotUsedDeltaNo.previous())
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during fetch locations from delta files.\n")
	}

	// Handle last delta file separately for fetch locations and walParser from it
	lastDeltaFile, err := getDeltaFile(reader, firstNotUsedDeltaNo.previous().getFilename(timeline))
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during downloading last delta file.\n")
	}
	deltaMap.AddLocationsToDelta(lastDeltaFile.Locations)

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
	lastUsedWalSegmentNo := NewWalSegmentNo(lastUsedLsn)
	return firstUsedWalSegmentNo, lastUsedWalSegmentNo.Next()
}

func handlFirstDeltaFileMiss(firstUsedDeltaNo DeltaNo,
	firstUsedWalSegmentNo WalSegmentNo,
	firstNotUsedWalSegmentNo WalSegmentNo,
	timeline uint32,
	reader internal.StorageFolderReader,
	deltaMap PagedFileDeltaMap) (error, *walparser.WalParser) {
	tracelog.InfoLogger.Printf("First delta file is missing, get locations from wals\n")
	/* The wal file of firstUsedWalSegmentNo should not contain a partitial XLogRecord in the head
	 * as it is the first wal file after the basebackup
	 */
	lastWalSegmengNo := firstUsedDeltaNo.firstWalSegmentNo().add(WalFileInDelta)
	if lastWalSegmengNo > firstNotUsedWalSegmentNo {
		lastWalSegmengNo = firstNotUsedWalSegmentNo
	}
	walparser := walparser.NewWalParser()
	err := deltaMap.getLocationsFromWals(reader, timeline, firstUsedWalSegmentNo,
		lastWalSegmengNo,
		walparser)
	return err, walparser
}
