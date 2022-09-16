package greenplum_test

import (
	"bytes"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/databases/greenplum"
	"github.com/wal-g/wal-g/internal/walparser/parsingutil"
	"io"
	"os"
	"testing"
)

const aoSegmentFileName = "../../../test/testdata/gp_ao_file.bin"
const aoSegmentFileSizeBytes = 192

func TestReadIncrement(t *testing.T) {
	gpReadIncrement(10, 100, t)
}

func TestReadIncrementFull(t *testing.T) {
	gpReadIncrement(0, aoSegmentFileSizeBytes, t)
}

func TestFailOnIncorrectOffset(t *testing.T) {
	file, err := os.Open(aoSegmentFileName)
	if err != nil {
		fmt.Print(err.Error())
	}

	_, err = greenplum.NewIncrementalPageReader(file, aoSegmentFileSizeBytes, aoSegmentFileSizeBytes)
	assert.Error(t, err)

	_, err = greenplum.NewIncrementalPageReader(file, 0, aoSegmentFileSizeBytes)
	assert.Error(t, err)
}

func gpReadIncrement(offset, eof int64, t *testing.T) {
	file, err := os.Open(aoSegmentFileName)
	if err != nil {
		fmt.Print(err.Error())
	}

	reader, err := greenplum.NewIncrementalPageReader(file, eof, offset)
	assert.NoError(t, err)

	increment, err := io.ReadAll(reader)
	assert.NoError(t, err)

	incrementBuf := bytes.NewBuffer(increment)
	err = greenplum.ReadIncrementFileHeader(incrementBuf)
	assert.NoError(t, err)

	var parsedEof uint64
	var parsedOffset uint64
	err = parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &parsedEof, Name: "eof"},
		{Field: &parsedOffset, Name: "offset"},
	}, incrementBuf)

	assert.Equal(t, parsedOffset, uint64(offset))
	assert.Equal(t, parsedEof, uint64(eof))

	_, _ = file.Seek(offset, io.SeekStart)

	fileFragment := new(bytes.Buffer)
	_, _ = io.CopyN(fileFragment, file, eof-offset)

	assert.True(t, bytes.Equal(fileFragment.Bytes(), incrementBuf.Bytes()))
}
