package postgres

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// buildValidChecksummedPage returns an 8KB page with a valid header and a correct checksum.
// pdLsnH=0, pdLsnL=100 → LSN = 100 (non-zero, so the page counts as initialized and valid).
func buildValidChecksummedPage(t *testing.T, blockNo uint32, path string) PgDatabasePage {
	t.Helper()
	var page PgDatabasePage

	binary.LittleEndian.PutUint32(page[0:], 0)       // pdLsnH
	binary.LittleEndian.PutUint32(page[4:], 100)     // pdLsnL → LSN = 100
	binary.LittleEndian.PutUint16(page[10:], 0)      // pdFlags
	binary.LittleEndian.PutUint16(page[12:], 28)     // pdLower
	binary.LittleEndian.PutUint16(page[14:], 8000)   // pdUpper
	binary.LittleEndian.PutUint16(page[16:], 8192)   // pdSpecial
	binary.LittleEndian.PutUint16(page[18:], 0x2005) // pdPageSizeVersion (size=8192, version=5)

	relFileID, err := GetRelFileIDFrom(path)
	require.NoError(t, err)
	checksum := pgChecksumPage(uint32(relFileID*BlocksInRelFile)+blockNo, &page)
	binary.LittleEndian.PutUint16(page[8:], checksum) // pdChecksum
	return page
}

// writeRelation writes pages to a file named like a PostgreSQL relation and returns its path.
func writeRelation(t *testing.T, pages ...PgDatabasePage) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "16384")
	var content []byte
	for _, page := range pages {
		content = append(content, page[:]...)
	}
	require.NoError(t, os.WriteFile(path, content, 0600))
	return path
}

// A page that is corrupt on disk and is not being written must be reported.
func TestVerifySinglePage_ReportsCorruptionWhenPageIsStable(t *testing.T) {
	page := buildValidChecksummedPage(t, 0, "16384")
	copy(page[6000:6004], []byte{0xDE, 0xAD, 0xBE, 0xEF})

	// The stream and the file hold the same corrupt bytes, so the re-read changes nothing.
	path := writeRelation(t, page)

	corrupted, err := verifySinglePage(path, 0, bytes.NewReader(page[:]))
	require.NoError(t, err)
	require.True(t, corrupted)
}

// A page that changed between the two reads is being written by PostgreSQL: the WAL holds a
// full page image, so it must not be reported.
func TestVerifySinglePage_SkipsPageBeingWritten(t *testing.T) {
	streamed := buildValidChecksummedPage(t, 0, "16384")
	copy(streamed[6000:6004], []byte{0xDE, 0xAD, 0xBE, 0xEF})

	// On disk the page already looks different — PostgreSQL moved on while we were reading.
	onDisk := buildValidChecksummedPage(t, 0, "16384")
	path := writeRelation(t, onDisk)

	corrupted, err := verifySinglePage(path, 0, bytes.NewReader(streamed[:]))
	require.NoError(t, err)
	require.False(t, corrupted)
}

// A truncated relation cannot be corrupt: the page is simply gone.
func TestVerifySinglePage_SkipsTruncatedRelation(t *testing.T) {
	page := buildValidChecksummedPage(t, 1, "16384")
	copy(page[6000:6004], []byte{0xDE, 0xAD, 0xBE, 0xEF})

	// The file holds a single block, so block 1 is past its end.
	first := buildValidChecksummedPage(t, 0, "16384")
	path := writeRelation(t, first)

	corrupted, err := verifySinglePage(path, 1, bytes.NewReader(page[:]))
	require.NoError(t, err)
	require.False(t, corrupted)
}

// A relation dropped while the backup was running is the same case as a truncated one:
// the page is gone, so it cannot be corrupt.
func TestVerifySinglePage_SkipsDroppedRelation(t *testing.T) {
	page := buildValidChecksummedPage(t, 0, "16384")
	copy(page[6000:6004], []byte{0xDE, 0xAD, 0xBE, 0xEF})

	path := filepath.Join(t.TempDir(), "16384")

	corrupted, err := verifySinglePage(path, 0, bytes.NewReader(page[:]))
	require.NoError(t, err)
	require.False(t, corrupted)
}

// A page with a correct checksum is never re-read.
func TestVerifySinglePage_AcceptsValidPage(t *testing.T) {
	page := buildValidChecksummedPage(t, 0, "16384")
	path := writeRelation(t, page)

	corrupted, err := verifySinglePage(path, 0, bytes.NewReader(page[:]))
	require.NoError(t, err)
	require.False(t, corrupted)
}
