package walparser_test

import (
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
