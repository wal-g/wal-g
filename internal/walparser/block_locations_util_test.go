package walparser_test

import (
	"github.com/wal-g/wal-g/internal/walparser"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/testtools"
)

func TestExtractBlockLocations(t *testing.T) {
	record, _ := testtools.GetXLogRecordData()
	expectedLocations := []BlockLocation{record.Blocks[0].Header.BlockLocation}
	actualLocations := walparser.ExtractBlockLocations([]XLogRecord{record})
	assert.Equal(t, expectedLocations, actualLocations)
}
