package walg

import (
	"bytes"
	"encoding/binary"
	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"io"
)

var IncrementScanUnexpectedEOF = errors.New("unexpected EOF during increment scan")
var IncrementFileHeader = []byte{'w', 'i', '1', SignatureMagicNumber}

// IncrementalPageReader constructs difference map during initialization and than re-read file
// Diff map can be of 1Gb/PostgresBlockSize elements == 512Kb
type IncrementalPageReader struct {
	backlog         chan []byte
	file            *io.LimitedReader
	pagedFileSeeker SeekerCloser
	FileSize        int64
	lsn             uint64
	next            *[]byte
	Blocks          []uint32
}

func (pageReader *IncrementalPageReader) Read(p []byte) (n int, err error) {
	err = nil
	if pageReader.next == nil {
		return 0, io.EOF
	}
	n = copy(p, *pageReader.next)
	if n == len(*pageReader.next) {
		pageReader.next = nil
	} else {
		data := (*(pageReader.next))[n:]
		pageReader.next = &(data)
	}

	if pageReader.next == nil {
		err = pageReader.drainMoreData()
	}

	return n, err
}

func (pageReader *IncrementalPageReader) drainMoreData() error {
	for len(pageReader.Blocks) > 0 && len(pageReader.backlog) < 2 {
		err := pageReader.advanceFileReader()
		if err != nil {
			return err
		}
	}

	if len(pageReader.backlog) > 0 {
		moreBytes := <-pageReader.backlog
		pageReader.next = &moreBytes
	}

	return nil
}

func (pageReader *IncrementalPageReader) advanceFileReader() error {
	pageBytes := make([]byte, WalPageSize)
	blockNo := pageReader.Blocks[0]
	pageReader.Blocks = pageReader.Blocks[1:]
	offset := int64(blockNo) * int64(WalPageSize)
	_, err := pageReader.pagedFileSeeker.Seek(offset, 0)
	if err != nil {
		return err
	}
	_, err = io.ReadFull(pageReader.file, pageBytes)
	if err == nil {
		pageReader.backlog <- pageBytes
	}
	return err
}

// Close IncrementalPageReader
func (pageReader *IncrementalPageReader) Close() error {
	return pageReader.pagedFileSeeker.Close()
}

// TODO : unit tests
func (pageReader *IncrementalPageReader) initialize(deltaBitmap *roaring.Bitmap) (size int64, err error) { // TODO : "initialize" is rather meaningless name, maybe this func should be decomposed
	size = 0
	// "wi" at the head stands for "wal-g increment"
	// format version "1", signature magic number
	pageReader.next = &IncrementFileHeader
	size += sizeofInt32
	fileSize := pageReader.FileSize
	pageReader.backlog <- toBytes(uint64(fileSize))
	size += sizeofInt64
	pageReader.Blocks = make([]uint32, 0, fileSize/int64(WalPageSize))

	if deltaBitmap == nil {
		err := pageReader.fullScanInitialize()
		if err != nil {
			return 0, err
		}
	} else {
		pageReader.DeltaBitmapInitialize(deltaBitmap)
	}

	size += pageReader.sendDiffMapToBacklog()
	pageReader.file.N = int64(len(pageReader.Blocks)) * int64(WalPageSize)
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
				_, err = pageReader.pagedFileSeeker.Seek(0, io.SeekStart)
				return err
			}
			return err
		}

		valid := pageReader.selectNewValidPage(pageBytes, currentBlockNumber)
		if !valid {
			return InvalidBlockError
		}
	}
}

func (pageReader *IncrementalPageReader) sendDiffMapToBacklog() (size int64) {
	diffBlockCount := len(pageReader.Blocks)
	pageReader.backlog <- toBytes(uint32(diffBlockCount))
	size = sizeofInt32

	diffMapSize := diffBlockCount * sizeofInt32
	var diffMap bytes.Buffer

	for _, blockNo := range pageReader.Blocks {
		binary.Write(&diffMap, binary.LittleEndian, blockNo)
	}

	pageReader.backlog <- diffMap.Bytes()
	size += int64(diffMapSize)
	size += int64(diffBlockCount) * int64(WalPageSize) // add data size
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
