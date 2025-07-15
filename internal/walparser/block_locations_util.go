package walparser

import (
	"bytes"
	"io"
)

func ExtractBlockLocations(records []XLogRecord) []BlockLocation {
	locations := make([]BlockLocation, 0)
	for _, record := range records {
		if record.IsZero() {
			continue
		}
		for _, block := range record.Blocks {
			locations = append(locations, block.Header.BlockLocation)
		}
	}
	return locations
}

// TODO : unit tests
func ExtractLocationsFromWalFile(parser *WalParser, walFile io.ReadCloser) ([]BlockLocation, error) {
	pageReader := NewWalPageReader(walFile)
	locations := make([]BlockLocation, 0)
	for {
		data, err := pageReader.ReadPageData()
		if err != nil {
			if err == io.EOF {
				return locations, nil
			}
			return nil, err
		}
		_, records, err := parser.ParseRecordsFromPage(bytes.NewReader(data))
		switch err.(type) {
		case nil:
		case PartialPageError:
		case ZeroPageError:
		default:
			return nil, err
		}
		locations = append(locations, ExtractBlockLocations(records)...)
	}
}
