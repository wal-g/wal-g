package innodb

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/wal-g/wal-g/internal/testutils"
	"github.com/wal-g/wal-g/testtools"
	"github.com/wal-g/wal-g/utility"
)

func generateData(t *testing.T) string {
	cwd, err := filepath.Abs("./")
	if err != nil {
		t.Log(err)
	}

	// Create temp directory.
	dir, err := os.MkdirTemp(cwd, "test_data")
	if err != nil {
		t.Log(err)
	}

	// Generates files with random data:
	files := []string{
		"orders.txt",
		"orders",
	}
	for _, file := range files {
		generateRandomFile(t, filepath.Join(dir, file))
	}

	// Generate innodb files with something like FSP-header:
	ibdFiles := []string{
		"main.ibd",
		"test.ibd",
		"orders.ibd",
	}
	for idx, file := range ibdFiles {
		generateInnodbFile(t, filepath.Join(dir, file), SpaceID(idx+1))
	}

	// add nested dir & nested file:
	err = os.MkdirAll(filepath.Join(dir, "nested"), 0777)
	if err != nil {
		t.Log(err)
	}
	generateInnodbFile(t, filepath.Join(dir, "nested", "database.ibd"), SpaceID(999))

	return dir
}

func generateRandomFile(t *testing.T, path string) {
	sb := testtools.NewStrideByteReader(10)
	lr := &io.LimitedReader{
		R: sb,
		N: int64(100),
	}
	f, err := os.Create(path)
	if err != nil {
		t.Log(err)
	}
	io.Copy(f, lr)
	defer utility.LoggedClose(f, "")
}

func generateInnodbFile(t *testing.T, path string, spaceId SpaceID) {
	sb := testtools.NewStrideByteReader(10)
	f, err := os.Create(path)
	if err != nil {
		t.Log(err)
	}

	var hexFile = `
		00000000  7c c3 d3 35 00 00 00 00  00 01 38 a3 00 00 00 01  ||..5......8.....|
		00000010  00 00 00 00 58 ed a9 c1  00 08 00 00 00 00 00 00  |....X...........|
		00000020  00 00 00 00 00 0b 00 00  00 0b 00 00 00 00 00 00  |................|
		00000030  5a 00 00 00 57 c0 00 00  40 21 00 00 00 3a 00 00  |Z...W...@!...:..|`
	pageBytes := testutils.HexToBytes(hexFile)
	// set SpaceID:
	binary.BigEndian.PutUint32(pageBytes[34:38], uint32(spaceId)) // FIL header
	binary.BigEndian.PutUint32(pageBytes[38:42], uint32(spaceId)) // FSP header
	io.Copy(f, bytes.NewReader(pageBytes))

	lr := &io.LimitedReader{
		R: sb,
		N: int64(100),
	}
	io.Copy(f, lr)
	defer utility.LoggedClose(f, "")
}

func TestSpaceIDCollector(t *testing.T) {
	tempDir := generateData(t)
	defer os.RemoveAll(tempDir)

	collector, err := NewSpaceIDCollector(tempDir)
	assert.NoError(t, err)

	raw := collector.(*spaceIDCollectorImpl).collected
	expected := map[SpaceID]string{
		SpaceID(1):   "main.ibd",
		SpaceID(2):   "test.ibd",
		SpaceID(3):   "orders.ibd",
		SpaceID(999): "nested/database.ibd",
	}

	assert.Equal(t, expected, raw)

}
