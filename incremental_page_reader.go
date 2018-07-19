package walg

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"github.com/pkg/errors"
)

var IncrementScanUnexpectedEOF = errors.New("unexpected EOF during increment scan")

// IncrementalPageReader constructs difference map during initialization and than re-read file
// Diff map can be of 1Gb/PostgresBlockSize elements == 512Kb
type IncrementalPageReader struct {
	backlog         chan []byte
	file            *io.LimitedReader
	pagedFileSeeker SeekerCloser
	info            os.FileInfo
	lsn             uint64
	next            *[]byte
	blocks          []uint32
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
	for len(pageReader.blocks) > 0 && len(pageReader.backlog) < 2 {
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
	blockNo := pageReader.blocks[0]
	pageReader.blocks = pageReader.blocks[1:]
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

func (pageReader *IncrementalPageReader) initialize() (size int64, err error) { // TODO : "initialize" is rather meaningless name, maybe this func should be decomposed
	size = 0
	// "wi" at the head stands for "wal-g increment"
	// format version "1", signature magic number
	pageReader.next = &[]byte{'w', 'i', '1', signatureMagicNumber}
	size += sizeofInt32
	fileSize := pageReader.info.Size()
	pageReader.backlog <- toBytes(uint64(fileSize))
	size += sizeofInt64

	pageBytes := make([]byte, WalPageSize)
	pageReader.blocks = make([]uint32, 0, fileSize/int64(WalPageSize))

	for currentBlockNumber := uint32(0); ; currentBlockNumber++ {
		n, err := io.ReadFull(pageReader.file, pageBytes)
		if err == io.ErrUnexpectedEOF || n%int(WalPageSize) != 0 { // TODO : seems that (n%int(WalPageSize) != 0) is useless here
			return 0, IncrementScanUnexpectedEOF
		}

		if err == io.EOF {
			size += pageReader.sendDiffMapToBacklog()

			_, err := pageReader.pagedFileSeeker.Seek(0, 0)
			if err != nil {
				return 0, nil
			}
			pageReader.file.N = int64(len(pageReader.blocks)) * int64(WalPageSize)
			return size, nil
		}

		if err != nil {
			return 0, err
		}

		valid := pageReader.selectNewValidPage(pageBytes, currentBlockNumber)
		if !valid {
			return 0, InvalidBlockError
		}
	}
}

func (pageReader *IncrementalPageReader) sendDiffMapToBacklog() (size int64) {
	diffBlockCount := len(pageReader.blocks)
	pageReader.backlog <- toBytes(uint32(diffBlockCount))
	size = sizeofInt32

	diffMapSize := diffBlockCount * sizeofInt32
	var diffMap bytes.Buffer

	for _, blockNo := range pageReader.blocks {
		binary.Write(&diffMap, binary.LittleEndian, blockNo)
	}

	pageReader.backlog <- diffMap.Bytes()
	size += int64(diffMapSize)
	size += int64(diffBlockCount) * int64(WalPageSize) // add data size
	return
}

// selectNewValidPage checks whether page is valid and if it so, then blockNo is appended to blocks list
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

	if (allZeroes) || (lsn >= pageReader.lsn) {
		pageReader.blocks = append(pageReader.blocks, blockNo)
	}
	return
}
