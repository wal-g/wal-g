package walparser_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/testtools"
)

func TestExtractBlockLocations(t *testing.T) {
	record, _ := testtools.GetXLogRecordData()
	expectedLocations := []walparser.BlockLocation{record.Blocks[0].Header.BlockLocation}
	actualLocations := walparser.ExtractBlockLocations([]walparser.XLogRecord{record})
	assert.Equal(t, expectedLocations, actualLocations)
}

func TestExtractLocationsFromWalFile(t *testing.T) {
	record, recordData := testtools.GetXLogRecordData()
	fileData := testtools.CreateWalPagesWithRecords(recordData)
	walFile := io.NopCloser(bytes.NewReader(fileData))
	expectedLocations := []walparser.BlockLocation{record.Blocks[0].Header.BlockLocation}
	actualLocations, err := walparser.ExtractLocationsFromWalFile(walparser.NewWalParser(), walFile)
	assert.NoError(t, err)
	assert.Equal(t, expectedLocations, actualLocations)
}

func TestExtractLocationsFromWalFile_MultipleRecords(t *testing.T) {
	record, recordData := testtools.GetXLogRecordData()
	fileData := testtools.CreateWalPagesWithRecords(recordData, recordData)
	walFile := io.NopCloser(bytes.NewReader(fileData))
	expectedLocations := []walparser.BlockLocation{
		record.Blocks[0].Header.BlockLocation, record.Blocks[0].Header.BlockLocation}
	actualLocations, err := walparser.ExtractLocationsFromWalFile(walparser.NewWalParser(), walFile)
	assert.NoError(t, err)
	assert.Equal(t, expectedLocations, actualLocations)
}
