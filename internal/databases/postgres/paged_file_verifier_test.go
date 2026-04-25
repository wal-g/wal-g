package postgres

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

// buildValidChecksummedPage returns an 8KB page with a valid header and correct checksum.
// pdLsnH=0, pdLsnL=100 → LSN = 100 (non-zero, satisfies isNew/isValid).
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

// TestPageVerifier_DetectsReplacement: overwrites 4 bytes (dd conv=notrunc, size stays 8192)
// → checksum mismatch → must be reported as corrupt.
func TestPageVerifier_DetectsReplacement(t *testing.T) {
	page := buildValidChecksummedPage(t, 0, "base/1/16384")

	copy(page[6000:6004], []byte{0xDE, 0xAD, 0xBE, 0xEF})

	v := newPageVerifier(false /* fullPageWrites */, LSN(0))
	corrupted, err := v.isPageCorrupted("base/1/16384", 0, &page)
	require.NoError(t, err)
	require.True(t, corrupted)
}

// TestPageVerifier_InvalidSizeOnInsertion: appending bytes (dd without conv=notrunc) makes
// the file size a non-multiple of the page size — hits the size-guard, not the checksum path.
func TestPageVerifier_InvalidSizeOnInsertion(t *testing.T) {
	page := buildValidChecksummedPage(t, 0, "base/1/16384")

	data := make([]byte, DatabasePageSize+4)
	copy(data, page[:DatabasePageSize])

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "base", "1", "16384")
	require.NoError(t, os.MkdirAll(filepath.Dir(filePath), 0o755))
	require.NoError(t, os.WriteFile(filePath, data, 0o644))

	fileInfo, err := os.Stat(filePath)
	require.NoError(t, err)

	f, err := os.Open(filePath)
	require.NoError(t, err)
	defer f.Close()

	v := newPageVerifier(false, LSN(0))
	corruptBlocks, err := v.verifyFile(filePath, fileInfo, f, false)
	require.NoError(t, err)
	require.Empty(t, corruptBlocks)
}

// TestPageVerifier_SkipsTornPage: a page with a bad checksum whose LSN > backupStartLSN
// must NOT be reported as corrupt when full_page_writes is on — it will be recovered via WAL FPI.
func TestPageVerifier_SkipsTornPage(t *testing.T) {
	// Page LSN = 100, backupStartLSN = 1 → lsn() > backupStartLSN.
	page := buildValidChecksummedPage(t, 0, "base/1/16384")
	copy(page[6000:6004], []byte{0xDE, 0xAD, 0xBE, 0xEF})

	v := newPageVerifier(true, LSN(1))
	corrupted, err := v.isPageCorrupted("base/1/16384", 0, &page)
	require.NoError(t, err)
	require.False(t, corrupted)
}

// TestPageVerifier_DetectsCorruptionWhenLSNBeforeBackup: full_page_writes=on does NOT protect
// pages written before the backup started — lsn() <= backupStartLSN → no FPI → must detect.
func TestPageVerifier_DetectsCorruptionWhenLSNBeforeBackup(t *testing.T) {
	page := buildValidChecksummedPage(t, 0, "base/1/16384")
	copy(page[6000:6004], []byte{0xDE, 0xAD, 0xBE, 0xEF})

	// Page LSN = 100, backupStartLSN = 200 → lsn() < backupStartLSN.
	v := newPageVerifier(true, LSN(200))
	corrupted, err := v.isPageCorrupted("base/1/16384", 0, &page)
	require.NoError(t, err)
	require.True(t, corrupted)
}
