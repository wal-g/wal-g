package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNextWALFileName(t *testing.T) {
	nextName, err := GetNextWalFilename("000000010000000000000051")
	assert.NoError(t, err)
	assert.Equal(t, "000000010000000000000052", nextName)

	nextName, err = GetNextWalFilename("00000001000000000000005F")
	assert.NoError(t, err)
	assert.Equal(t, "000000010000000000000060", nextName)

	nextName, err = GetNextWalFilename("0000000100000001000000FF")
	assert.NoError(t, err)
	assert.Equal(t, "000000010000000200000000", nextName)

	_, err = GetNextWalFilename("0000000100000001000001FF")
	assert.Errorf(t, err, "TestNextWALFileName 0000000100000001000001FF did not failed")

	_, err = GetNextWalFilename("00000001000ZZ001000000FF")
	assert.Errorf(t, err, "TestNextWALFileName 00000001000ZZ001000001FF did not failed")

	_, err = GetNextWalFilename("00000001000001000000FF")
	assert.Errorf(t, err, "TestNextWALFileName 00000001000001000001FF did not failed")

	_, err = GetNextWalFilename("asdfasdf")
	assert.Errorf(t, err, "TestNextWALFileName asdfasdf did not failed")
}

func TestPrefetchLocation(t *testing.T) {
	prefetchLocation, runningLocation, runningFile, fetchedFile := getPrefetchLocations("/var/pgdata/xlog/", "000000010000000000000051")
	assert.Equal(t, "/var/pgdata/xlog/.wal-g/prefetch", prefetchLocation)
	assert.Equal(t, "/var/pgdata/xlog/.wal-g/prefetch/running", runningLocation)
	assert.Equal(t, "/var/pgdata/xlog/.wal-g/prefetch/running/000000010000000000000051", runningFile)
	assert.Equal(t, "/var/pgdata/xlog/.wal-g/prefetch/000000010000000000000051", fetchedFile)
}

func testParseWALFilenameError(t *testing.T, WALFilename string) {
	_, _, err := ParseWALFilename(WALFilename)
	assert.Errorf(t, err, "TestParseWALFilename %s did not fail", WALFilename)
}

func testParseWALFilenameCorrect(t *testing.T, WALFilename string, expectedTimelineID uint32, expectedLogSegNo uint64) {
	actualTimelineID, actualLogSegNo, err := ParseWALFilename(WALFilename)
	assert.NoErrorf(t, err, "TestParseWALFilename %s failed", WALFilename)
	assert.Equal(t, expectedLogSegNo, actualLogSegNo)
	assert.Equal(t, expectedTimelineID, actualTimelineID)
}
func TestParseWALFilename(t *testing.T) {
	testParseWALFilenameError(t, "00000001")
	testParseWALFilenameError(t, "000000010000000100000100000000010000000100000001")
	testParseWALFilenameError(t, "000000010000000100000100")
	testParseWALFilenameError(t, "000000010000000110000001")
	testParseWALFilenameError(t, "000xYz010000000100000001")
	testParseWALFilenameError(t, "0000000100xYz00100000001")
	testParseWALFilenameError(t, "000000010000000100xYz001")

	testParseWALFilenameCorrect(t, "000000010000000100000001", 1, 1<<8+1)
	testParseWALFilenameCorrect(t, "000000100000020000000030", 1<<4, 2<<16+3<<4)
	testParseWALFilenameCorrect(t, "10000000f0000000000000a0", 1<<28, 15<<36+10<<4)
	testParseWALFilenameCorrect(t, "ffffffffffffffff000000ff", 1<<32-1, 1<<40-1)
}

func TestParseWALFilenameDifferentSize(t *testing.T) {
	SetWalSize(64)
	testParseWALFilenameError(t, "10000000f0000000000000a0")

	testParseWALFilenameCorrect(t, "000000010000000000000001", 1, 1)
	testParseWALFilenameCorrect(t, "000000100000000100000001", 1<<4, 4<<4+1)
	testParseWALFilenameCorrect(t, "000000100000020000000030", 1<<4, 2<<14+3<<4)
	testParseWALFilenameCorrect(t, "10000000f000000000000030", 1<<28, 15<<34+3<<4)
}
