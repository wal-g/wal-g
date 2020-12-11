package walparser_test

import (
	"bytes"
	"github.com/wal-g/wal-g/internal/walparser"
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

var locations = []BlockLocation{
	*NewBlockLocation(1, 2, 3, 4),
	*NewBlockLocation(5, 6, 7, 8),
}

func TestReadWrite(t *testing.T) {
	var buf bytes.Buffer
	writer := walparser.NewBlockLocationWriter(&buf)
	reader := walparser.NewBlockLocationReader(&buf)
	for _, location := range locations {
		err := writer.WriteLocation(location)
		assert.NoError(t, err)
	}
	actualLocations := make([]BlockLocation, 0)
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
	err := walparser.WriteLocationsTo(&buf, locations)
	assert.NoError(t, err)
	reader := walparser.NewBlockLocationReader(&buf)
	actualLocations := make([]BlockLocation, 0)
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
	err := walparser.WriteLocationsTo(&buf, locations)
	assert.NoError(t, err)
	actualLocations, err := walparser.ReadLocationsFrom(&buf)
	assert.NoError(t, err)
	assert.Equal(t, locations, actualLocations)
}
