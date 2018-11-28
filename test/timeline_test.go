package test

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal"
	"testing"
)

func TestNextWALFileName(t *testing.T) {
	nextName, err := internal.GetNextWalFilename("000000010000000000000051")
	assert.NoError(t, err)
	assert.Equal(t, "000000010000000000000052", nextName)

	nextName, err = internal.GetNextWalFilename("00000001000000000000005F")
	assert.NoError(t, err)
	assert.Equal(t, "000000010000000000000060", nextName)

	nextName, err = internal.GetNextWalFilename("0000000100000001000000FF")
	assert.NoError(t, err)
	assert.Equal(t, "000000010000000200000000", nextName)

	_, err = internal.GetNextWalFilename("0000000100000001000001FF")
	assert.Errorf(t, err, "TestNextWALFileName 0000000100000001000001FF did not failed")

	_, err = internal.GetNextWalFilename("00000001000ZZ001000000FF")
	assert.Errorf(t, err, "TestNextWALFileName 00000001000ZZ001000001FF did not failed")

	_, err = internal.GetNextWalFilename("00000001000001000000FF")
	assert.Errorf(t, err, "TestNextWALFileName 00000001000001000001FF did not failed")

	_, err = internal.GetNextWalFilename("asdfasdf")
	assert.Errorf(t, err, "TestNextWALFileName asdfasdf did not failed")
}

func TestPrefetchLocation(t *testing.T) {
	prefetchLocation, runningLocation, runningFile, fetchedFile := internal.GetPrefetchLocations("/var/pgdata/xlog/", "000000010000000000000051")
	assert.Equal(t, "/var/pgdata/xlog/.wal-g/prefetch", prefetchLocation)
	assert.Equal(t, "/var/pgdata/xlog/.wal-g/prefetch/running", runningLocation)
	assert.Equal(t, "/var/pgdata/xlog/.wal-g/prefetch/running/000000010000000000000051", runningFile)
	assert.Equal(t, "/var/pgdata/xlog/.wal-g/prefetch/000000010000000000000051", fetchedFile)
}
