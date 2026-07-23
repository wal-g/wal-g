package greenplum

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/zeebo/xxh3"
)

func checksumOfPrefix(data []byte, n int64) string {
	hasher := xxh3.New128()
	hasher.Write(data[:n])
	return hex.EncodeToString(hasher.Sum(nil))
}

func writeMockAoFile(t *testing.T, name string, data []byte) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, name)
	require.NoError(t, os.WriteFile(path, data, 0o600))
	return path
}

func makeMockData(size int) []byte {
	data := make([]byte, size)
	for i := range data {
		data[i] = byte(i % 251) // 251 is prime => nice spread, avoids trivial repetition
	}
	return data
}

// TestValidateFileChecksum_ChecksumMatches_ShouldIncrement verifies that when the checksum of
// the local file over the base EOF matches the previously stored checksum,
// ValidateFileChecksum signals that an incremental upload is possible and returns the checksum
// computed over the current (grown) EOF.
func TestValidateFileChecksum_ChecksumMatches_ShouldIncrement(t *testing.T) {
	const oldEof = int64(60)
	const curEof = int64(100)
	data := makeMockData(int(curEof))
	path := writeMockAoFile(t, "1663.1", data)

	previousChecksum := checksumOfPrefix(data, oldEof)
	expectedNewChecksum := checksumOfPrefix(data, curEof)

	checksum, shouldIncrement, err := validateFileChecksum(context.Background(), path, oldEof, curEof, previousChecksum)

	assert.NoError(t, err)
	assert.True(t, shouldIncrement, "expected incremental upload to be allowed when base checksums match")
	assert.Equal(t, expectedNewChecksum, checksum, "returned checksum must be computed over the current EOF")
}

// TestValidateFileChecksum_ChecksumDiffers_NoIncrement verifies that when the previously
// stored checksum does not match the checksum of the local file over the base
// EOF, ValidateFileChecksum reports that no incremental upload should happen (a regular upload
// is required) and returns an empty checksum without an error.
func TestValidateFileChecksum_ChecksumDiffers_NoIncrement(t *testing.T) {
	const oldEof = int64(60)
	const curEof = int64(100)
	data := makeMockData(int(curEof))
	path := writeMockAoFile(t, "1663.1", data)

	// A checksum that will not match the actual prefix checksum.
	previousChecksum := "deadbeefdeadbeefdeadbeefdeadbeef"

	checksum, shouldIncrement, err := validateFileChecksum(context.Background(), path, oldEof, curEof, previousChecksum)

	assert.NoError(t, err)
	assert.False(t, shouldIncrement, "incremental upload must not happen when base checksums differ")
	assert.Empty(t, checksum, "checksum must be empty when incremental upload is rejected")
}

// TestValidateFileChecksum_EmptyPreviousChecksum_NoIncrement verifies that when the previous
// checksum is empty (e.g. it was never stored for the remote base file), ValidateFileChecksum
// falls back to a regular upload even if the prefix would otherwise match.
func TestValidateFileChecksum_EmptyPreviousChecksum_NoIncrement(t *testing.T) {
	const oldEof = int64(60)
	const curEof = int64(100)
	data := makeMockData(int(curEof))
	path := writeMockAoFile(t, "1663.1", data)

	checksum, shouldIncrement, err := validateFileChecksum(context.Background(), path, oldEof, curEof, "")

	assert.NoError(t, err)
	assert.False(t, shouldIncrement, "empty previous checksum must force a regular upload")
	assert.Empty(t, checksum)
}

// TestValidateFileChecksum_OldEofBeyondFileSize_ReturnsError verifies that when the requested
// base EOF exceeds the actual file size, the checksum computation fails (short
// read) and ValidateFileChecksum propagates the error without allowing an incremental upload.
func TestValidateFileChecksum_OldEofBeyondFileSize_ReturnsError(t *testing.T) {
	data := makeMockData(50)
	path := writeMockAoFile(t, "1663.1", data)

	// oldEof is larger than the file, so io.CopyN inside getCheckSum fails.
	const oldEof = int64(100)
	const curEof = int64(120)

	previousChecksum := checksumOfPrefix(data, int64(len(data)))

	checksum, shouldIncrement, err := validateFileChecksum(context.Background(), path, oldEof, curEof, previousChecksum)

	assert.Error(t, err, "reading beyond the file size must produce an error")
	assert.False(t, shouldIncrement)
	assert.Empty(t, checksum)
}

// TestValidateFileChecksum_CurEofBeyondFileSize_ReturnsError verifies that when the base
// checksum matches but the current EOF exceeds the file size, the second
// getCheckSum call fails and ValidateFileChecksum propagates the error.
func TestValidateFileChecksum_CurEofBeyondFileSize_ReturnsError(t *testing.T) {
	const oldEof = int64(60)
	data := makeMockData(80)
	path := writeMockAoFile(t, "1663.1", data)

	previousChecksum := checksumOfPrefix(data, oldEof)

	// curEof exceeds the file size, so the second getCheckSum call fails.
	const curEof = int64(200)

	checksum, shouldIncrement, err := validateFileChecksum(context.Background(), path, oldEof, curEof, previousChecksum)

	assert.Error(t, err, "reading current EOF beyond the file size must produce an error")
	assert.False(t, shouldIncrement)
	assert.Empty(t, checksum)
}

// TestValidateFileChecksum_EqualEof_ShouldIncrement verifies the boundary case where the base
// EOF equals the current EOF (no growth). Since the checksums match, ValidateFileChecksum
// still allows the (degenerate) incremental upload and returns the same checksum.
func TestValidateFileChecksum_EqualEof_ShouldIncrement(t *testing.T) {
	const eof = int64(100)
	data := makeMockData(int(eof))
	path := writeMockAoFile(t, "1663.1", data)

	previousChecksum := checksumOfPrefix(data, eof)

	checksum, shouldIncrement, err := validateFileChecksum(context.Background(), path, eof, eof, previousChecksum)

	assert.NoError(t, err)
	assert.True(t, shouldIncrement)
	assert.Equal(t, previousChecksum, checksum, "checksum over identical EOFs must be identical")
}

func TestGetChecksum_IntendedUsage(t *testing.T) {
	eof := int64(60)
	data := makeMockData(int(eof))
	path := writeMockAoFile(t, "1663.1", data)
	expectedChecksum := checksumOfPrefix(data, eof)

	checksum, err := getCheckSum(t.Context(), path, eof)

	assert.NoError(t, err)
	assert.Equal(t, expectedChecksum, checksum, "returned checksum must be computed over the current EOF")
}

func TestGetChecksum_FileIsLonger(t *testing.T) {
	eof := int64(60)
	data := makeMockData(int(eof + 30))
	path := writeMockAoFile(t, "1663.1", data)
	expectedChecksum := checksumOfPrefix(data, eof)

	checksum, err := getCheckSum(t.Context(), path, eof)

	assert.NoError(t, err)
	assert.Equal(t, expectedChecksum, checksum, "returned checksum must be computed over the current EOF")
}

func TestGetChecksum_FileNotExist(t *testing.T) {
	_, err := getCheckSum(t.Context(), "random_path", 64)
	assert.Error(t, err)
}

func TestGetChecksum_EofLongerThanFile(t *testing.T) {
	eof := int64(60)
	data := makeMockData(int(eof))
	path := writeMockAoFile(t, "1663.1", data)

	_, err := getCheckSum(t.Context(), path, eof+30)

	assert.Error(t, err)
}
