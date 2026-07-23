package greenplum

import (
	"context"
	"encoding/hex"
	"io"
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

	err, checksum, shouldIncrement := validateFileChecksum(context.Background(), path, oldEof, curEof, previousChecksum)

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

	err, checksum, shouldIncrement := validateFileChecksum(context.Background(), path, oldEof, curEof, previousChecksum)

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

	err, checksum, shouldIncrement := validateFileChecksum(context.Background(), path, oldEof, curEof, "")

	assert.NoError(t, err)
	assert.False(t, shouldIncrement, "empty previous checksum must force a regular upload")
	assert.Empty(t, checksum)
}

// TestValidateFileChecksum_FileNotExist_ReturnsError verifies that a missing file leads to an
// error being returned (from the first getCheckSum call), no incremental upload,
// and an empty checksum.
func TestValidateFileChecksum_FileNotExist_ReturnsError(t *testing.T) {
	const oldEof = int64(60)
	const curEof = int64(100)

	dir := t.TempDir()
	missingPath := filepath.Join(dir, "does_not_exist.1")

	err, checksum, shouldIncrement := validateFileChecksum(context.Background(), missingPath, oldEof, curEof, "somechecksum")

	assert.Error(t, err, "a missing file must produce an error")
	assert.False(t, shouldIncrement)
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

	err, checksum, shouldIncrement := validateFileChecksum(context.Background(), path, oldEof, curEof, previousChecksum)

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

	err, checksum, shouldIncrement := validateFileChecksum(context.Background(), path, oldEof, curEof, previousChecksum)

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

	err, checksum, shouldIncrement := validateFileChecksum(context.Background(), path, eof, eof, previousChecksum)

	assert.NoError(t, err)
	assert.True(t, shouldIncrement)
	assert.Equal(t, previousChecksum, checksum, "checksum over identical EOFs must be identical")
}

func hashViaTeeLimited(t *testing.T, data []byte, eof int64) (string, int) {
	t.Helper()

	hasher := xxh3.New128()
	tee := io.TeeReader(byteReader(data), newLimitedWriter(hasher, eof))

	streamed, err := io.Copy(io.Discard, tee)
	require.NoError(t, err)

	return hex.EncodeToString(hasher.Sum(nil)), int(streamed)
}

func byteReader(data []byte) io.Reader {
	return &sliceReader{data: data}
}

type sliceReader struct {
	data []byte
	off  int
}

func (r *sliceReader) Read(p []byte) (int, error) {
	if r.off >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.off:])
	r.off += n
	return n, nil
}

// TestRegularUploadChecksum_StopsAtEOF verifies that the streaming checksum only
// covers the first `eof` bytes even when the file has extra trailing bytes, and
// that it exactly matches checksumOfPrefix (the same thing getCheckSum computes).
func TestRegularUploadChecksum_StopsAtEOF(t *testing.T) {
	const eof = int64(100)
	// File is larger than EOF: the tail simulates garbage from aborted txns.
	data := makeMockData(160)

	got, streamed := hashViaTeeLimited(t, data, eof)

	assert.Equal(t, len(data), streamed, "the whole file must still be streamed to storage")
	assert.Equal(t, checksumOfPrefix(data, eof), got,
		"streaming checksum must cover only the first eof bytes, matching getCheckSum")
	assert.NotEqual(t, checksumOfPrefix(data, int64(len(data))), got,
		"streaming checksum must NOT depend on bytes past eof")
}

// TestRegularUploadChecksum_ExactEOF verifies the boundary case where the file
// size equals EOF: hashing the whole file and hashing up to EOF are identical.
func TestRegularUploadChecksum_ExactEOF(t *testing.T) {
	const eof = int64(128)
	data := makeMockData(int(eof))

	got, streamed := hashViaTeeLimited(t, data, eof)

	assert.Equal(t, len(data), streamed)
	assert.Equal(t, checksumOfPrefix(data, eof), got)
}

// TestLimitedWriter_ReportsFullLengthAndCapsHashing verifies the limitedWriter
// contract directly: it forwards at most `limit` bytes to the underlying writer
// but always reports the full input length so an io.TeeReader never sees a short
// write.
func TestLimitedWriter_ReportsFullLengthAndCapsHashing(t *testing.T) {
	hasher := xxh3.New128()
	lw := newLimitedWriter(hasher, 5)

	// First write is within the limit.
	n, err := lw.Write([]byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, 5, n)

	// Second write is entirely past the limit; it must be reported as fully
	// consumed but must not reach the hasher.
	n, err = lw.Write([]byte("world"))
	require.NoError(t, err)
	assert.Equal(t, 5, n, "writer must report full length even when discarding")

	expected := xxh3.New128()
	expected.Write([]byte("hello"))
	assert.Equal(t, hex.EncodeToString(expected.Sum(nil)), hex.EncodeToString(hasher.Sum(nil)),
		"only the first `limit` bytes must be hashed")
}

// TestLimitedWriter_PartialWriteAcrossLimit verifies the case where a single
// Write straddles the limit: part of it is hashed, the rest discarded, and the
// full length is reported.
func TestLimitedWriter_PartialWriteAcrossLimit(t *testing.T) {
	hasher := xxh3.New128()
	lw := newLimitedWriter(hasher, 3)

	n, err := lw.Write([]byte("abcdef"))
	require.NoError(t, err)
	assert.Equal(t, 6, n, "full input length must be reported")

	expected := xxh3.New128()
	expected.Write([]byte("abc"))
	assert.Equal(t, hex.EncodeToString(expected.Sum(nil)), hex.EncodeToString(hasher.Sum(nil)),
		"only bytes up to the limit must be hashed")
}
