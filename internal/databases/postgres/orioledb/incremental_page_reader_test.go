package orioledb

import (
	"bytes"
	"encoding/binary"
	"io"
	"testing"

	"github.com/RoaringBitmap/roaring/v2"
)

type mockReadSeekCloser struct {
	*bytes.Reader
}

func (m *mockReadSeekCloser) Close() error {
	return nil
}

func newMockReadSeekCloser(data []byte) *mockReadSeekCloser {
	return &mockReadSeekCloser{Reader: bytes.NewReader(data)}
}

func TestIncrementalPageReader_Initialize(t *testing.T) {
	t.Run("FullScanUncompressed", func(t *testing.T) {
		pageSize := DatabasePageSize
		pageData := make([]byte, pageSize)
		binary.LittleEndian.PutUint32(pageData[0:4], 100)

		mockFile := newMockReadSeekCloser(pageData)
		pageReader := &incrementalPageReader{
			PagedFile: mockFile,
			FileSize:  pageSize,
			ChkpNum:   50,
		}

		size, err := pageReader.initialize(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if pageReader.Compressed {
			t.Errorf("expected Compressed to be false")
		}

		if len(pageReader.Blocks) != 1 || pageReader.Blocks[0] != 0 {
			t.Errorf("expected Blocks [0], got %v", pageReader.Blocks)
		}

		expectedHeaderLen := len(IncrementFileHeader) + 8 + 2 + 4 + 4
		if len(pageReader.Next) != expectedHeaderLen {
			t.Errorf("expected header length %d, got %d", expectedHeaderLen, len(pageReader.Next))
		}

		expectedTotalSize := int64(len(pageReader.Next)) + int64(len(pageReader.Blocks))*pageSize
		if size != expectedTotalSize {
			t.Errorf("expected total size %d, got %d", expectedTotalSize, size)
		}
	})

	t.Run("DeltaBitmap", func(t *testing.T) {
		pageSize := DatabasePageSize
		fileSize := pageSize * 5

		mockFile := newMockReadSeekCloser(make([]byte, fileSize))
		pageReader := &incrementalPageReader{
			PagedFile: mockFile,
			FileSize:  fileSize,
			ChkpNum:   0,
		}

		bitmap := roaring.BitmapOf(0, 2, 10)

		size, err := pageReader.initialize(bitmap)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		expectedBlocks := []uint32{0, 2}
		if len(pageReader.Blocks) != len(expectedBlocks) {
			t.Fatalf("expected %d blocks, got %d", len(expectedBlocks), len(pageReader.Blocks))
		}

		for i, block := range expectedBlocks {
			if pageReader.Blocks[i] != block {
				t.Errorf("at index %d: expected block %d, got %d", i, block, pageReader.Blocks[i])
			}
		}

		expectedTotalSize := int64(len(pageReader.Next)) + int64(len(pageReader.Blocks))*pageSize
		if size != expectedTotalSize {
			t.Errorf("expected size %d, got %d", expectedTotalSize, size)
		}
	})

	t.Run("ShortFileHeader", func(t *testing.T) {
		mockFile := newMockReadSeekCloser([]byte{0x01, 0x02})
		pageReader := &incrementalPageReader{
			PagedFile: mockFile,
			FileSize:  2,
		}

		size, err := pageReader.initialize(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if pageReader.Compressed {
			t.Errorf("expected Compressed to be false")
		}

		if size != int64(len(pageReader.Next)) {
			t.Errorf("expected total size %d, got %d", len(pageReader.Next), size)
		}
	})

	t.Run("HeaderFormat", func(t *testing.T) {
		fileSize := DatabasePageSize
		pageData := make([]byte, fileSize)
		binary.LittleEndian.PutUint32(pageData[0:4], 100)

		mockFile := newMockReadSeekCloser(pageData)
		pageReader := &incrementalPageReader{
			PagedFile: mockFile,
			FileSize:  fileSize,
			ChkpNum:   50,
		}

		_, err := pageReader.initialize(nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		buf := bytes.NewReader(pageReader.Next)

		magic := make([]byte, len(IncrementFileHeader))
		if _, err := io.ReadFull(buf, magic); err != nil || !bytes.Equal(magic, IncrementFileHeader) {
			t.Errorf("invalid header magic: got %v, want %v", magic, IncrementFileHeader)
		}

		var readFileSize uint64
		if err := binary.Read(buf, binary.LittleEndian, &readFileSize); err != nil || int64(readFileSize) != fileSize {
			t.Errorf("invalid fileSize in header: got %d, want %d", readFileSize, fileSize)
		}

		var readPageSize uint16
		if err := binary.Read(buf, binary.LittleEndian, &readPageSize); err != nil || int64(readPageSize) != DatabasePageSize {
			t.Errorf("invalid pageSize in header: got %d, want %d", readPageSize, DatabasePageSize)
		}

		var blockCount uint32
		if err := binary.Read(buf, binary.LittleEndian, &blockCount); err != nil || blockCount != 1 {
			t.Errorf("invalid block count in header: got %d, want 1", blockCount)
		}

		var blockNo uint32
		if err := binary.Read(buf, binary.LittleEndian, &blockNo); err != nil || blockNo != 0 {
			t.Errorf("invalid block number in header: got %d, want 0", blockNo)
		}
	})
}