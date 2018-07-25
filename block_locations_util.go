package walg

import "github.com/wal-g/wal-g/walparser"

func extractBlockLocations(records []walparser.XLogRecord) []walparser.BlockLocation {
	locations := make([]walparser.BlockLocation, 0)
	for _, record := range records {
		for _, block := range record.Blocks {
			locations = append(locations, block.Header.BlockLocation)
		}
	}
	return locations
}

func uniqueLocations(locations []walparser.BlockLocation) []walparser.BlockLocation {
	uniqueLocations := make([]walparser.BlockLocation, 0)
	recordedLocations := make(map[walparser.BlockLocation]bool)
	for _, location := range locations {
		if _, ok := recordedLocations[location]; !ok {
			uniqueLocations = append(uniqueLocations, location)
		}
		recordedLocations[location] = true
	}
	return uniqueLocations
}
