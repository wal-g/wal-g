package innodb

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/testutils"
	"testing"
)

func TestFSP(t *testing.T) {
	// 0 page of file-per-table file:
	var hexFile = `
		00000000  7c c3 d3 35 00 00 00 00  00 01 38 a3 00 00 00 01  ||..5......8.....|
		00000010  00 00 00 00 58 ed a9 c1  00 08 00 00 00 00 00 00  |....X...........|
		00000020  00 00 00 00 00 0b 00 00  00 0b 00 00 00 00 00 00  |................|
		00000030  5a 00 00 00 57 c0 00 00  40 21 00 00 00 3a 00 00  |Z...W...@!...:..|`

	pageBytes := testutils.HexToBytes(hexFile)
	assert.Equal(t, PageTypeFileSpaceHeader, readHeader(pageBytes).PageType)
	header := readFileSpaceHeader(pageBytes)
	// parsed by innodb_ruby:
	// #<struct Innodb::Page::FspHdrXdes::Header
	// space_id=11,
	// unused=0,
	// size=23040,
	// free_limit=22464,
	// flags=
	//  #<struct Innodb::Page::FspHdrXdes::Flags
	//   system_page_size=16384,
	//   compressed=true,
	//   page_size=16384,
	//   post_antelope=true,
	//   atomic_blobs=true,
	//   data_directory=false,
	//   value=16417>,
	// frag_n_used=58,
	assert.Equal(t, FileSpaceHeader{
		SpaceID:                      11,
		HighestPageNumberInFile:      23040,
		HighestPageNumberInitialized: 22464,
		Flags:                        16417,
	}, header)
	assert.Equal(t, uint16(0), header.Flags.pageSize())
	assert.Equal(t, uint16(0), header.Flags.compressedPageSize())
	assert.Equal(t, false, header.Flags.isDataDir())
}
