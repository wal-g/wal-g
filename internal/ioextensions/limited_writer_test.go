package ioextensions_test

import (
	"encoding/hex"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/zeebo/xxh3"
)

// TestLimitedWriter_ReportsFullLengthAndCapsHashing verifies the limitedWriter
// contract directly: it forwards at most `limit` bytes to the underlying writer
// but always reports the full input length to avoid a short write.
func TestLimitedWriter_ReportsFullLengthAndCapsHashing(t *testing.T) {
	hasher := xxh3.New128()
	lw := ioextensions.NewLimitedWriter(hasher, 5)

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
	lw := ioextensions.NewLimitedWriter(hasher, 3)

	n, err := lw.Write([]byte("abcdef"))
	require.NoError(t, err)
	assert.Equal(t, 6, n, "full input length must be reported")

	expected := xxh3.New128()
	expected.Write([]byte("abc"))
	assert.Equal(t, hex.EncodeToString(expected.Sum(nil)), hex.EncodeToString(hasher.Sum(nil)),
		"only bytes up to the limit must be hashed")
}
