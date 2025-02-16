package xbstream

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wal-g/wal-g/internal/testutils"
)

func TestBuildFakeDiff(t *testing.T) {
	pageSize := 16 * 1024
	header := testutils.HexToBytes(`
				00000000  58 54 52 41 00 00 00 03  00 00 00 04 00 00 00 05  |XTRA............|
				00000010  00 00 00 06 00 00 00 07  00 00 00 08 00 00 00 09  |................|
				00000020  00 00 00 0a 00 00 00 0b  00 00 00 0c 00 00 00 0d  |................|
				00000030  00 00 00 0e 00 00 00 0f  00 00 00 10 00 00 00 11  |................|
				00000040  00 00 00 12 00 00 00 13  00 00 00 14 00 00 00 15  |................|`)
	page := generateBytes(pageSize)
	expected := testutils.HexToBytes(`
				00000000  58 54 52 41 00 00 00 03  ff ff ff ff 00 00 00 00  |XTRA............|
				00000010  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|`)
	expected = append(expected, make([]byte, pageSize-32)...)
	expected = append(expected, page...)

	sink := &diffFileSink{
		meta: &deltaMetadata{PageSize: uint32(pageSize)},
	}
	actual := sink.buildFakeDelta(header, page)
	assert.Equal(t, expected, actual)
}

func generateBytes(size int) []byte {
	result := make([]byte, size)
	for i := 0; i < size; i++ {
		result[i] = byte(i % 256)
	}
	return result
}
