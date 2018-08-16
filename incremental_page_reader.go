package walg

import (
	"bytes"
	"encoding/binary"
	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"io"
)

var IncrementScanUnexpectedEOF = errors.New("unexpected EOF during increment scan")
// "wi" at the head stands for "wal-g increment"
// format version "1", signature magic number
var IncrementFileHeader = []byte{'w', 'i', '1', SignatureMagicNumber}

// IncrementalPageReader constructs difference map during initialization and than re-read file
// Diff map can be of 1Gb/PostgresBlockSize elements == 512Kb
type IncrementalPageReader struct {
	file            *io.LimitedReader
	pagedFileSeeker SeekerCloser
	FileSize        int64
	lsn             uint64
	next            []byte
	Blocks          []uint32
}

// TODO : unit tests
func (pageReader *IncrementalPageReader) Read(p []byte) (n int, err error) {
	for {
		copied := copy(p, pageReader.next)
		p = p[copied:]
		pageReader.next = pageReader.next[copied:]
		n += copied
		if len(p) == 0 {
			return n, nil
		}
		moreData, err := pageReader.drainMoreData()
		if err != nil {
			return n, err
		}
		if !moreData {
			return n, io.EOF
		}
	}
}

// TODO : unit tests
func (pageReader *IncrementalPageReader) drainMoreData() (bool, error) {
	if len(pageReader.Blocks) == 0 {
		return false, nil
	}
	err := pageReader.advanceFileReader()
	if err != nil {
		return false, err
	}
	return true, nil
}

// TODO : unit tests
func (pageReader *IncrementalPageReader) advanceFileReader() error {
	pageBytes := make([]byte, WalPageSize)
	blockNo := pageReader.Blocks[0]
	pageReader.Blocks = pageReader.Blocks[1:]
	offset := int64(blockNo) * int64(WalPageSize)
	_, err := pageReader.pagedFileSeeker.Seek(offset, io.SeekStart) // TODO : possible race condition - page was deleted between blocks extraction and seek
	if err != nil {
		return err
	}
	_, err = io.ReadFull(pageReader.file, pageBytes)
	if err == nil {
		pageReader.next = pageBytes
	}
	return err
}

// Close IncrementalPageReader
func (pageReader *IncrementalPageReader) Close() error {
	return pageReader.pagedFileSeeker.Close()
}

// TODO : unit tests
func (pageReader *IncrementalPageReader) initialize(deltaBitmap *roaring.Bitmap) (size int64, err error) { // TODO : "initialize" is rather meaningless name, maybe this func should be decomposed
	var headerBuffer bytes.Buffer
	headerBuffer.Write(IncrementFileHeader)
	fileSize := pageReader.FileSize
	headerBuffer.Write(toBytes(uint64(fileSize)))
	pageReader.Blocks = make([]uint32, 0, fileSize/int64(WalPageSize))

	if deltaBitmap == nil {
		err := pageReader.fullScanInitialize()
		if err != nil {
			return 0, err
		}
	} else {
		pageReader.DeltaBitmapInitialize(deltaBitmap)
	}

	pageReader.writeDiffMapToHeader(&headerBuffer)
	pageReader.next = headerBuffer.Bytes()
	pageDataSize := int64(len(pageReader.Blocks)) * int64(WalPageSize)
	size = int64(headerBuffer.Len()) + pageDataSize
	pageReader.file.N = pageDataSize
	return
}

func (pageReader *IncrementalPageReader) DeltaBitmapInitialize(deltaBitmap *roaring.Bitmap) {
	it := deltaBitmap.Iterator()
	for it.HasNext() {
		blockNo := it.Next()
		if pageReader.FileSize >= int64(blockNo+1)*int64(WalPageSize) { // whole block fits into file
			pageReader.Blocks = append(pageReader.Blocks, blockNo)
		} else {
			break
		}
	}
}

// TODO : unit tests
func (pageReader *IncrementalPageReader) fullScanInitialize() error {
	pageBytes := make([]byte, WalPageSize)
	for currentBlockNumber := uint32(0); ; currentBlockNumber++ {
		_, err := io.ReadFull(pageReader.file, pageBytes)
		if err == io.ErrUnexpectedEOF {
			return IncrementScanUnexpectedEOF
		}

		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		valid := pageReader.selectNewValidPage(pageBytes, currentBlockNumber)
		if !valid {
			return InvalidBlockError
		}
	}
}

func (pageReader *IncrementalPageReader) writeDiffMapToHeader(headerWriter io.Writer) {
	diffBlockCount := len(pageReader.Blocks)
	headerWriter.Write(toBytes(uint32(diffBlockCount)))

	for _, blockNo := range pageReader.Blocks {
		binary.Write(headerWriter, binary.LittleEndian, blockNo)
	}
	return
}

// TODO : unit tests
// selectNewValidPage checks whether page is valid and if it so, then blockNo is appended to Blocks list
func (pageReader *IncrementalPageReader) selectNewValidPage(pageBytes []byte, blockNo uint32) (valid bool) {
	pageHeader, _ := ParsePostgresPageHeader(bytes.NewReader(pageBytes))
	valid = pageHeader.IsValid()
	lsn := pageHeader.Lsn()

	allZeroes := false

	if !valid {
		if allZero(pageBytes) {
			allZeroes = true
		} else {
			return false
		}
	}

	if allZeroes || (lsn >= pageReader.lsn) {
		pageReader.Blocks = append(pageReader.Blocks, blockNo)
	}
	return
}
