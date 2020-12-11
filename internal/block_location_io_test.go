package internal_test

import (
	"bytes"
	"github.com/wal-g/wal-g/internal/fsutil"
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/walparser"
)

var locations = []walparser.BlockLocation{
	*walparser.NewBlockLocation(1, 2, 3, 4),
	*walparser.NewBlockLocation(5, 6, 7, 8),
}

func TestReadWrite(t *testing.T) {
	var buf bytes.Buffer
	writer := fsutil.NewBlockLocationWriter(&buf)
	reader := fsutil.NewBlockLocationReader(&buf)
	for _, location := range locations {
		err := writer.WriteLocation(location)
		assert.NoError(t, err)
	}
	actualLocations := make([]walparser.BlockLocation, 0)
	for {
		location, err := reader.ReadNextLocation()
		if errors.Cause(err) == io.EOF {
			break
		}
		assert.NoError(t, err)
		actualLocations = append(actualLocations, *location)
	}
	assert.Equal(t, locations, actualLocations)
}

func TestWriteLocationsTo(t *testing.T) {
	var buf bytes.Buffer
	err := fsutil.WriteLocationsTo(&buf, locations)
	assert.NoError(t, err)
	reader := fsutil.NewBlockLocationReader(&buf)
	actualLocations := make([]walparser.BlockLocation, 0)
	for {
		location, err := reader.ReadNextLocation()
		if errors.Cause(err) == io.EOF {
			break
		}
		assert.NoError(t, err)
		actualLocations = append(actualLocations, *location)
	}
	assert.Equal(t, locations, actualLocations)
}

func TestReadLocationsFrom(t *testing.T) {
	var buf bytes.Buffer
	err := fsutil.WriteLocationsTo(&buf, locations)
	assert.NoError(t, err)
	actualLocations, err := fsutil.ReadLocationsFrom(&buf)
	assert.NoError(t, err)
	assert.Equal(t, locations, actualLocations)
}
