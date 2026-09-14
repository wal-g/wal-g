package innodb

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRepairSparsePreservesCompressedPage(t *testing.T) {
	for _, blockSize := range []int64{1, 512, 4096, 32768} {
		t.Run(fmt.Sprint(blockSize), func(t *testing.T) {
			testRepairSparsePreservesCompressedPage(t, blockSize)
		})
	}
}

func testRepairSparsePreservesCompressedPage(t *testing.T, blockSize int64) {
	// FIL_PAGE_COMPRESS_SIZE_V1 stores only the zlib payload length. The
	// uncompressed FIL header precedes it, just as in a COMPRESSION='zlib'
	// tablespace extracted from a compressed XtraBackup archive.
	payload := bytes.Repeat([]byte("mysql page data "), InnoDBDefaultPageSize)
	payload = payload[:InnoDBDefaultPageSize-FILHeaderSize]
	var compressed bytes.Buffer
	writer := zlib.NewWriter(&compressed)
	_, err := writer.Write(payload)
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	page := make([]byte, InnoDBDefaultPageSize)
	binary.BigEndian.PutUint32(page[4:8], 1)
	binary.BigEndian.PutUint16(page[24:26], uint16(PageTypeCompressed))
	page[26], page[27] = 2, 1 // compression version 2, zlib
	binary.BigEndian.PutUint16(page[28:30], uint16(PageTypeIndex))
	binary.BigEndian.PutUint16(page[30:32], uint16(len(payload)))
	binary.BigEndian.PutUint16(page[32:34], uint16(compressed.Len()))
	copy(page[FILHeaderSize:], compressed.Bytes())
	// XtraBackup preserves padding within the last filesystem block. Keep
	// it nonzero so the test also detects unaligned hole punching.
	paddingEnd := (int64(FILHeaderSize+compressed.Len()) + blockSize - 1) / blockSize * blockSize
	if paddingEnd > InnoDBDefaultPageSize {
		paddingEnd = InnoDBDefaultPageSize
	}
	for i := FILHeaderSize + compressed.Len(); i < int(paddingEnd); i++ {
		page[i] = 0x5a
	}

	first := make([]byte, InnoDBDefaultPageSize)
	binary.BigEndian.PutUint16(first[24:26], uint16(PageTypeFileSpaceHeader))
	want := append(first, page...)
	file, err := os.Create(filepath.Join(t.TempDir(), "table.ibd"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, file.Close()) })
	_, err = file.Write(want)
	require.NoError(t, err)

	calls := 0
	err = repairSparse(file, blockSize, func(file *os.File, offset, length int64) error {
		calls++
		// Linux FALLOC_FL_PUNCH_HOLE zeroes the entire requested range,
		// including partial filesystem blocks. Emulate it on every platform.
		_, err := file.WriteAt(make([]byte, length), offset)
		return err
	})
	require.NoError(t, err)
	if blockSize < InnoDBDefaultPageSize {
		require.Equal(t, 1, calls)
	} else {
		require.Zero(t, calls, "no whole filesystem block fits inside this page")
	}
	got, err := os.ReadFile(file.Name())
	require.NoError(t, err)
	require.Len(t, got, len(want))
	end := InnoDBDefaultPageSize + FILHeaderSize + compressed.Len()
	reader, err := zlib.NewReader(bytes.NewReader(got[InnoDBDefaultPageSize+FILHeaderSize : end]))
	require.NoError(t, err)
	restored, err := io.ReadAll(reader)
	require.NoError(t, err, "hole punching must not overwrite the compressed payload")
	require.NoError(t, reader.Close())
	require.Equal(t, payload, restored)
	require.True(t, bytes.Equal(want, got), "repairing sparse allocation must preserve page bytes")
}
