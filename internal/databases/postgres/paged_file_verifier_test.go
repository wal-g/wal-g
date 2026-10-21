package postgres

import (
	"bytes"
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// Named arguments for the retryOnChecksumMismatch parameter of verifySinglePage.
const (
	retryEnabled  = true
	retryDisabled = false
)

// buildValidChecksummedPage returns an 8KB page with a valid header and a correct checksum.
// segmentBlockOffset is the absolute block number of the segment's first block: 0 for the
// first segment file, BlocksInRelFile for the ".1" file, and so on.
// pdLsnH=0, pdLsnL=100 → LSN = 100 (non-zero, so the page counts as initialized and valid).
func buildValidChecksummedPage(t *testing.T, blockNo, segmentBlockOffset uint32) PgDatabasePage {
	t.Helper()
	var page PgDatabasePage

	binary.LittleEndian.PutUint32(page[0:], 0)       // pdLsnH
	binary.LittleEndian.PutUint32(page[4:], 100)     // pdLsnL → LSN = 100
	binary.LittleEndian.PutUint16(page[10:], 0)      // pdFlags
	binary.LittleEndian.PutUint16(page[12:], 28)     // pdLower
	binary.LittleEndian.PutUint16(page[14:], 8000)   // pdUpper
	binary.LittleEndian.PutUint16(page[16:], 8192)   // pdSpecial
	binary.LittleEndian.PutUint16(page[18:], 0x2005) // pdPageSizeVersion (size=8192, version=5)

	checksum := pgChecksumPage(segmentBlockOffset+blockNo, &page)
	binary.LittleEndian.PutUint16(page[8:], checksum) // pdChecksum
	return page
}

// writeRelation writes pages to a file named like a PostgreSQL relation and returns its path.
func writeRelation(t *testing.T, pages ...PgDatabasePage) string {
	t.Helper()
	return writeSegment(t, "16384", pages...)
}

// writeSegment writes pages to base/16384/<name>, the layout isChecksumValidatableFile expects,
// and returns the path.
func writeSegment(t *testing.T, name string, pages ...PgDatabasePage) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "base", "16384")
	require.NoError(t, os.MkdirAll(dir, 0700))
	path := filepath.ToSlash(filepath.Join(dir, name))
	var content []byte
	for _, page := range pages {
		content = append(content, page[:DatabasePageSize]...)
	}
	require.NoError(t, os.WriteFile(path, content, 0600))
	return path
}

// A page that is corrupt on disk and is not being written must be reported.
func TestVerifySinglePage_ReportsCorruptionWhenPageIsStable(t *testing.T) {
	page := buildValidChecksummedPage(t, 0, 0)
	copy(page[6000:6004], []byte{0xDE, 0xAD, 0xBE, 0xEF})

	// The stream and the file hold the same corrupt bytes, so the re-read changes nothing.
	path := writeRelation(t, page)

	corrupted, err := verifySinglePage(path, 0, 0, bytes.NewReader(page[:]), retryEnabled)
	require.NoError(t, err)
	require.True(t, corrupted)
}

// A page that changed between the two reads is being written by PostgreSQL: the WAL holds a
// full page image, so it must not be reported.
func TestVerifySinglePage_SkipsPageBeingWritten(t *testing.T) {
	streamed := buildValidChecksummedPage(t, 0, 0)
	copy(streamed[6000:6004], []byte{0xDE, 0xAD, 0xBE, 0xEF})

	// On disk the page already looks different — PostgreSQL moved on while we were reading.
	onDisk := buildValidChecksummedPage(t, 0, 0)
	path := writeRelation(t, onDisk)

	corrupted, err := verifySinglePage(path, 0, 0, bytes.NewReader(streamed[:]), retryEnabled)
	require.NoError(t, err)
	require.False(t, corrupted)
}

// A truncated relation cannot be corrupt: the page is simply gone.
func TestVerifySinglePage_SkipsTruncatedRelation(t *testing.T) {
	page := buildValidChecksummedPage(t, 1, 0)
	copy(page[6000:6004], []byte{0xDE, 0xAD, 0xBE, 0xEF})

	// The file holds a single block, so block 1 is past its end.
	first := buildValidChecksummedPage(t, 0, 0)
	path := writeRelation(t, first)

	corrupted, err := verifySinglePage(path, 1, 0, bytes.NewReader(page[:]), retryEnabled)
	require.NoError(t, err)
	require.False(t, corrupted)
}

// A relation dropped while the backup was running is the same case as a truncated one:
// the page is gone, so it cannot be corrupt.
func TestVerifySinglePage_SkipsDroppedRelation(t *testing.T) {
	page := buildValidChecksummedPage(t, 0, 0)
	copy(page[6000:6004], []byte{0xDE, 0xAD, 0xBE, 0xEF})

	path := filepath.ToSlash(filepath.Join(t.TempDir(), "16384"))

	corrupted, err := verifySinglePage(path, 0, 0, bytes.NewReader(page[:]), retryEnabled)
	require.NoError(t, err)
	require.False(t, corrupted)
}

// A page with a correct checksum is never re-read.
func TestVerifySinglePage_AcceptsValidPage(t *testing.T) {
	page := buildValidChecksummedPage(t, 0, 0)
	path := writeRelation(t, page)

	corrupted, err := verifySinglePage(path, 0, 0, bytes.NewReader(page[:]), retryEnabled)
	require.NoError(t, err)
	require.False(t, corrupted)
}

// With the re-read turned off, a page that changed between the two reads is still reported:
// that is the point of WALG_VERIFY_PAGE_CHECKSUMS_RETRY=false.
func TestVerifySinglePage_ReportsCorruptionWhenRetryDisabled(t *testing.T) {
	streamed := buildValidChecksummedPage(t, 0, 0)
	copy(streamed[6000:6004], []byte{0xDE, 0xAD, 0xBE, 0xEF})

	onDisk := buildValidChecksummedPage(t, 0, 0)
	path := writeRelation(t, onDisk)

	corrupted, err := verifySinglePage(path, 0, 0, bytes.NewReader(streamed[:]), retryDisabled)
	require.NoError(t, err)
	require.True(t, corrupted)
}

// A re-read that stops in the middle of the block is not a rewritten page: the original
// verdict must stand.
func TestVerifySinglePage_ReportsCorruptionOnShortReread(t *testing.T) {
	page := buildValidChecksummedPage(t, 0, 0)
	copy(page[6000:6004], []byte{0xDE, 0xAD, 0xBE, 0xEF})

	// The file is cut in the middle of the block, so the re-read returns only half of it.
	path := writeRelation(t, page)
	require.NoError(t, os.Truncate(path, DatabasePageSize/2))

	corrupted, err := verifySinglePage(path, 0, 0, bytes.NewReader(page[:]), retryEnabled)
	require.NoError(t, err)
	require.True(t, corrupted)
}

// The checksum of a page in the second segment file covers its absolute block number, so the
// segment offset computed once per file in verifyPageBlocks must reach the per-page check.
func TestVerifyPagedFileBase_UsesSegmentBlockOffset(t *testing.T) {
	page := buildValidChecksummedPage(t, 0, uint32(BlocksInRelFile))
	path := writeSegment(t, "16384.1", page)
	fileInfo, err := os.Stat(path)
	require.NoError(t, err)

	corruptBlocks, err := VerifyPagedFileBase(path, fileInfo, bytes.NewReader(page[:DatabasePageSize]), retryEnabled)
	require.NoError(t, err)
	require.Empty(t, corruptBlocks)
}
