package innodb

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/testutils"
	"testing"
)

func TestFILHeader(t *testing.T) {
	// 0 page of file-per-table file:
	var hexFile = `
		00000000  7c c3 d3 35 00 00 00 00  00 01 38 a3 00 00 00 01  ||..5......8.....|
		00000010  00 00 00 00 58 ed a9 c1  00 08 00 00 00 00 00 00  |....X...........|
		00000020  00 00 00 00 00 0b 00 00  00 0b 00 00 00 00 00 00  |................|`

	pageBytes := testutils.HexToBytes(hexFile)
	header := readHeader(pageBytes)
	// parsed by innodb_ruby:
	// #<struct Innodb::Page::FilHeader
	//	 checksum=2093208373,
	//	 offset=0,
	//	 prev=80035,
	//	 next=1,
	//	 lsn=1491970497,
	//	 type=:FSP_HDR,
	//	 flush_lsn=0,
	//	 space_id=11>
	expected := FILHeader{
		Checksum:        2093208373,
		PageNumber:      PageNumber(0),
		PreviousPage:    80035,
		NextPage:        1,
		LastModifiedLSN: 1491970497,
		PageType:        PageTypeFileSpaceHeader,
		FlushLSN:        0,
		SpaceID:         11,
	}
	assert.Equal(t, expected, header)
}

func TestFILHeader_Compressed(t *testing.T) {
	// 0 page of file-per-table file:
	var hexFile = `
		00000000  6d 70 f2 97 00 00 00 01  00 00 00 00 00 00 00 00  |mp..............|
		00000010  00 00 00 00 30 ad ff 90  00 0e 02 01 00 05 3f da  |....0.........?.|
		00000020  00 35 00 00 00 0b 78 9c  ed c1 41 11 00 20 08 00  |.5....x...A.. ..|`

	pageBytes := testutils.HexToBytes(hexFile)
	header := readHeader(pageBytes)
	// parsed by innodb_ruby:
	// #<struct Innodb::Page::FilHeader
	//  checksum=1836118679,
	//  offset=1,
	//  prev=0,
	//  next=0,
	//  lsn=816709520,
	//  type=:COMPRESSED,
	//  flush_lsn=144396685598654517,
	//  space_id=11>
	expected := FILHeader{
		Checksum:        1836118679,
		PageNumber:      PageNumber(1),
		PreviousPage:    0,
		NextPage:        0,
		LastModifiedLSN: 816709520,
		PageType:        PageTypeCompressed,
		FlushLSN:        144396685598654517,
		SpaceID:         11,
	}
	assert.Equal(t, expected, header)

	meta := expected.GetCompressedData()
	assert.Equal(t, CompressedMeta{
		Version:         53,
		CompressionAlgo: 0,
		OrigPageType:    PageTypeAllocated,
		OrigDataSize:    0,
		CompressedSize:  0},
		meta)
}
