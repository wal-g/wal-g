package splitmerge

import (
	"bytes"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
)

func getSample(size int) io.Reader {
	out := make([]byte, size)
	for i := 0; i < size; i++ {
		out[i] = byte(i)
	}
	return bytes.NewReader(out)
}

func testReaders(t *testing.T, readers <-chan io.ReadCloser, size int) {
	var idx byte = 0
	for reader := range readers {
		data, err := io.ReadAll(reader)
		assert.NoError(t, err)

		for _, val := range data {
			assert.Equal(t, val, idx)
			idx += 1
		}
	}

	assert.Equal(t, idx, byte(size))
}

func TestSplitMaxSizeReader1(t *testing.T) {
	const size = 39
	sample := getSample(size)
	readers := SplitMaxSizeReader(sample, 3, 11)

	testReaders(t, readers, size)
}

func TestSplitMaxSizeReader2(t *testing.T) {
	const size = 11
	sample := getSample(size)
	readers := SplitMaxSizeReader(sample, 3, 39)

	testReaders(t, readers, size)
}

func TestSplitMaxSizeReader3(t *testing.T) {
	const size = 11
	sample := getSample(size)
	readers := SplitMaxSizeReader(sample, 17, 39)

	testReaders(t, readers, size)
}

func TestSplitMaxSizeReader4(t *testing.T) {
	const size = 11
	sample := getSample(size)
	readers := SplitMaxSizeReader(sample, 17, 39)

	testReaders(t, readers, size)
}

func TestSplitMaxSizeReader5(t *testing.T) {
	const size = 39
	sample := getSample(size)
	readers := SplitMaxSizeReader(sample, 11, 11)

	testReaders(t, readers, size)
}
