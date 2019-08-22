package internal

import (
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/internal/storages/storage"
	"github.com/wal-g/wal-g/internal/walparser"
)

func GetDeltaMap(folder storage.Folder, timeline uint32, firstUsedLSN, firstNotUsedLSN uint64) (PagedFileDeltaMap, error) {
	deltaMap := NewPagedFileDeltaMap()
	firstUsedDeltaNo, firstNotUsedDeltaNo := GetDeltaRange(firstUsedLSN, firstNotUsedLSN)
	// Get locations from [firstUsedDeltaNo, lastUsedDeltaNo). We use lastUsedDeltaNo in next step
	locationsFromDeltas, err := GetLocationsFromDeltas(folder, timeline, firstUsedDeltaNo, firstNotUsedDeltaNo.Previous())
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during fetch locations from delta files.\n")
	}

	// Handle last delta file separately for fetch locations and walParser from it
	lastDeltaFile, err := getDeltaFile(folder, firstNotUsedDeltaNo.Previous().GetFilename(timeline))
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during downloading last delta file.\n")
	}

	firstUsedWalSegmentNo, firstNotUsedWalSegmentNo := GetWalSegmentRange(firstNotUsedDeltaNo, firstNotUsedLSN)
	// we handle WAL files from [firstUsedWalSegmentNo, lastUsedWalSegmentNo]
	locationsFromWals, err := getLocationsFromWals(folder, timeline, firstUsedWalSegmentNo, firstNotUsedWalSegmentNo, lastDeltaFile.WalParser)
	if err != nil {
		return deltaMap, errors.Wrapf(err, "Error during fetch locations from wal segments.\n")
	}

	deltaMap.AddLocationsToDelta(locationsFromDeltas)
	deltaMap.AddLocationsToDelta(lastDeltaFile.Locations)
	deltaMap.AddLocationsToDelta(locationsFromWals)
	return deltaMap, nil
}

func GetLocationsFromDeltas(folder storage.Folder, timeline uint32, first, last DeltaNo) (
	[]walparser.BlockLocation, error) {
	locations := make([]walparser.BlockLocation, 0)
	for deltaNo := first; deltaNo < last; deltaNo = deltaNo.Next() {
		filename := deltaNo.GetFilename(timeline)
		deltaFile, err := getDeltaFile(folder, filename)
		if err != nil {
			return nil, err
		}
		locations = append(locations, deltaFile.Locations...)
	}
	return locations, nil
}

func getLocationsFromWals(folder storage.Folder, timeline uint32, first, last WalSegmentNo, walParser *walparser.WalParser) (
	[]walparser.BlockLocation, error) {
	locations := make([]walparser.BlockLocation, 0)
	for walSegmentNo := first; walSegmentNo < last; walSegmentNo = walSegmentNo.Next() {
		filename := walSegmentNo.GetFilename(timeline)
		locationsFromFile, err := getLocationsFromWal(folder, filename, walParser)
		if err != nil {
			return nil, err
		}
		locations = append(locations, locationsFromFile...)
	}
	return locations, nil
}

func getLocationsFromWal(folder storage.Folder, filename string, walParser *walparser.WalParser) ([]walparser.BlockLocation, error) {
	reader, err := DownloadAndDecompressWALFile(folder, filename)
	if err != nil {
		return nil, errors.Wrapf(err, "Error during wal segment'%s' downloading.", filename)
	}
	locations, err := extractLocationsFromWalFile(walParser, reader)
	if err != nil {
		return nil, errors.Wrapf(err, "Error during extracting locations from wal segment: '%s'", filename)
	}
	err = reader.Close()
	if err != nil {
		return nil, errors.Wrapf(err, "Error during reading wal segment '%s'", filename)
	}
	return locations, nil
}

func getDeltaFile(folder storage.Folder, filename string) (*DeltaFile, error) {
	reader, err := DownloadAndDecompressWALFile(folder, filename)
	if err != nil {
		return nil, errors.Wrapf(err, "Error during delta file '%s' downloading.", filename)
	}
	deltaFile, err := LoadDeltaFile(reader)
	if err != nil {
		return nil, errors.Wrapf(err, "Error during extracting locations from delta file: '%s'", filename)
	}
	err = reader.Close()
	if err != nil {
		return nil, errors.Wrapf(err, "Error during reading delta file '%s'", filename)
	}
	return deltaFile, nil
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