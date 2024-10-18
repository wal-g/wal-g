package xbstream

import (
	"github.com/stretchr/testify/assert"
	"github.com/wal-g/wal-g/internal/testutils"
	"testing"
)

func TestMetadataParser(t *testing.T) {
	var tests = []struct {
		testName       string
		rawFileContent []byte // hexdump -C mysql.ibd.meta
		expected       diffMetadata
	}{
		{
			testName: "idb file",
			rawFileContent: testutils.HexToBytes(`
				00000000  70 61 67 65 5f 73 69 7a  65 20 3d 20 31 36 33 38  |page_size = 1638|
				00000010  34 0a 7a 69 70 5f 73 69  7a 65 20 3d 20 30 0a 73  |4.zip_size = 0.s|
				00000020  70 61 63 65 5f 69 64 20  3d 20 38 0a 73 70 61 63  |pace_id = 8.spac|
				00000030  65 5f 66 6c 61 67 73 20  3d 20 33 33 0a           |e_flags = 33.|`),
			expected: diffMetadata{
				PageSize:   16 * 1024,
				ZipSize:    0,
				SpaceID:    8,
				SpaceFlags: 33,
			},
		},
		{
			testName: "undo file",
			rawFileContent: testutils.HexToBytes(`
				00000000  70 61 67 65 5f 73 69 7a  65 20 3d 20 31 36 33 38  |page_size = 1638|
				00000010  34 0a 7a 69 70 5f 73 69  7a 65 20 3d 20 30 0a 73  |4.zip_size = 0.s|
				00000020  70 61 63 65 5f 69 64 20  3d 20 34 32 39 34 39 36  |pace_id = 429496|
				00000030  37 32 37 38 0a 73 70 61  63 65 5f 66 6c 61 67 73  |7278.space_flags|
				00000040  20 3d 20 30 0a                                    | = 0.|`),
			expected: diffMetadata{
				PageSize:   16 * 1024,
				ZipSize:    0,
				SpaceID:    4294967278,
				SpaceFlags: 0,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.testName, func(t *testing.T) {
			actual, err := parseDiffMetadata(tt.rawFileContent)
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
