package internal

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"

	"github.com/RoaringBitmap/roaring"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
	"github.com/getlantern/deepcopy"
)

// "wi" at the head stands for "wal-g increment"
// format version "1", signature magic number
var IncrementFileHeader = []byte{'w', 'i', '1', SignatureMagicNumber}

// IncrementalPageReader constructs difference map during initialization and than re-read file
// Diff map may consist of 1Gb/PostgresBlockSize elements == 512Kb
type IncrementalPageReader struct {
	PagedFile ioextensions.ReadSeekCloser
	FileSize  int64
	Lsn       uint64
	Next      []byte
	Blocks    []uint32
}

func (pageReader *IncrementalPageReader) Read(p []byte) (n int, err error) {
	for {
		copied := copy(p, pageReader.Next)
		p = p[copied:]
		pageReader.Next = pageReader.Next[copied:]
		n += copied
		if len(p) == 0 {
			return n, nil
		}
		moreData, err := pageReader.DrainMoreData()
		if err != nil {
			return n, err
		}
		if !moreData {
			return n, io.EOF
		}
	}
}

func (pageReader *IncrementalPageReader) DrainMoreData() (succeed bool, err error) {
	if len(pageReader.Blocks) == 0 {
		return false, nil
	}
	err = pageReader.AdvanceFileReader()
	if err != nil {
		return false, err
	}
	return true, nil
}

func (pageReader *IncrementalPageReader) AdvanceFileReader() error {
	pageBytes := make([]byte, DatabasePageSize)
	blockNo := pageReader.Blocks[0]
	pageReader.Blocks = pageReader.Blocks[1:]
	offset := int64(blockNo) * int64(DatabasePageSize)
	// TODO : possible race condition - page was deleted between blocks extraction and seek
	_, err := pageReader.PagedFile.Seek(offset, io.SeekStart)
	if err != nil {
		return err
	}
	_, err = io.ReadFull(pageReader.PagedFile, pageBytes)
	if err == nil {
		pageReader.Next = pageBytes
	}
	return err
}

// Close IncrementalPageReader
func (pageReader *IncrementalPageReader) Close() error {
	return pageReader.PagedFile.Close()
}

// TODO : unit tests
// TODO : "initialize" is rather meaningless name, maybe this func should be decomposed
func (pageReader *IncrementalPageReader) initialize(deltaBitmap *roaring.Bitmap, pageReader1 *IncrementalPageReader, pageReader2 *IncrementalPageReader) (size int64, err error) {
	var headerBuffer bytes.Buffer
	headerBuffer.Write(IncrementFileHeader)
	fileSize := pageReader.FileSize
	headerBuffer.Write(utility.ToBytes(uint64(fileSize)))
	pageReader.Blocks = make([]uint32, 0, fileSize/int64(DatabasePageSize))

	if deltaBitmap == nil {
		err := pageReader.FullScanInitialize()
		if err != nil {
			return 0, err
		}
	} else {
		tracelog.InfoLogger.Println("right branch")
		pageReader1.Blocks = make([]uint32, 0, fileSize/int64(DatabasePageSize))
		pageReader2.Blocks = make([]uint32, 0, fileSize/int64(DatabasePageSize))
		tracelog.InfoLogger.Println("init blocks")
		pageReader1.DeltaBitmapInitialize2(deltaBitmap)
		tracelog.InfoLogger.Println("delta init")
		err = pageReader2.FullScanInitialize2()
		if err != nil {
			tracelog.ErrorLogger.Fatalf("Keka3: %v", err)
		}
		tracelog.InfoLogger.Println("full init")
		blocks := diff(pageReader2.Blocks, pageReader1.Blocks)
		tracelog.InfoLogger.Println("diff success")
		err = pageReader1.PrintDiff(blocks)
		if err != nil {
			tracelog.ErrorLogger.Fatalf("Keka4: %v", err)
		}
		tracelog.InfoLogger.Println("print diff success")




		for i := 0; i < len(pageReader.Blocks) && i < len(pageReader2.Blocks); i++ {
			tracelog.InfoLogger.Println(pageReader1.Blocks[i])
			tracelog.InfoLogger.Println(pageReader2.Blocks[i])
			if pageReader1.Blocks[i] != pageReader2.Blocks[i] {
				tracelog.WarningLogger.Printf("Met different blocks: delta %d and full %d \n", pageReader1.Blocks[i], pageReader2.Blocks[i])
				blocks = append(blocks, pageReader2.Blocks[i])
			}
		}

		if len(pageReader1.Blocks) > len(pageReader2.Blocks) {
			blocks = append(blocks, pageReader1.Blocks[len(pageReader2.Blocks):]...)
		} else if len(pageReader1.Blocks) < len(pageReader2.Blocks) {
			blocks = append(blocks, pageReader2.Blocks[len(pageReader1.Blocks):]...)
		}
		if len(pageReader1.Blocks) != len(pageReader2.Blocks) {
			tracelog.WarningLogger.Printf("Blocks have different len: delta %d and full %d \n", len(pageReader1.Blocks), len(pageReader2.Blocks))
			for block := range blocks {
				tracelog.WarningLogger.Printf("Different block: %d \n", block)
			}
		}


		pageReader.DeltaBitmapInitialize(deltaBitmap)

	}

	pageReader.WriteDiffMapToHeader(&headerBuffer)
	pageReader.Next = headerBuffer.Bytes()
	pageDataSize := int64(len(pageReader.Blocks)) * int64(DatabasePageSize)
	size = int64(headerBuffer.Len()) + pageDataSize
	return
}

func diff(first, second []uint32) []uint32 {
	d := make([]uint32, 0)
	for i := 0; i < len(first); i++ {
		found := false
		for j := 0; j < len(second); j++ {
			if second[j] == first[i] {
				found = true
				break
			}
		}
		if !found {
			d = append(d, first[i])
			tracelog.InfoLogger.Printf("First has %d block", first[i])
		}
	}
	return d
}

func (pageReader *IncrementalPageReader) DeltaBitmapInitialize(deltaBitmap *roaring.Bitmap) {
	it := deltaBitmap.Iterator()
	for it.HasNext() { // TODO : do something with file truncation during reading
		blockNo := it.Next()
		if pageReader.FileSize >= int64(blockNo+1)*int64(DatabasePageSize) { // whole block fits into file
			pageReader.Blocks = append(pageReader.Blocks, blockNo)
		} else {
			break
		}
	}
}

func (pageReader *IncrementalPageReader) FullScanInitialize() error {
	pageBytes := make([]byte, DatabasePageSize)
	for currentBlockNumber := uint32(0); ; currentBlockNumber++ {
		_, err := io.ReadFull(pageReader.PagedFile, pageBytes)

		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}

		valid := pageReader.SelectNewValidPage(pageBytes, currentBlockNumber) // TODO : torn page possibility
		if !valid {
			return NewInvalidBlockError(currentBlockNumber)
		}
	}
}

func (pageReader *IncrementalPageReader) PrintDiff(diff []uint32) error {
	pageBytes := make([]byte, DatabasePageSize)
	if diff == nil || len(diff) == 0 {
		tracelog.InfoLogger.Println("Diff is empty")
		return nil
	}
	for currentBlockNumber := uint32(0); ; currentBlockNumber++ {
		found := false
		for i := 0; i < len(diff); i++ {
			if currentBlockNumber == diff[i] {
				found = true
				break
			}
		}

		_, err := io.ReadFull(pageReader.PagedFile, pageBytes)

		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}
		if !found {
			break
		}

		valid := pageReader.SelectNewValidPage2(pageBytes, currentBlockNumber) // TODO : torn page possibility
		if !valid {
			return NewInvalidBlockError(currentBlockNumber)
		}
	}
	return nil
}

func (pageReader *IncrementalPageReader) FullScanInitialize2() error {
	pageBytes := make([]byte, DatabasePageSize)
	for currentBlockNumber := uint32(0); ; currentBlockNumber++ {
		_, err := io.ReadFull(pageReader.PagedFile, pageBytes)

		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}

		valid := pageReader.SelectNewValidPage(pageBytes, currentBlockNumber) // TODO : torn page possibility
		if !valid {
			return NewInvalidBlockError(currentBlockNumber)
		}
		tracelog.InfoLogger.Printf("Full scan, block no: %d\n", currentBlockNumber)
	}
}

func (pageReader *IncrementalPageReader) DeltaBitmapInitialize2(deltaBitmap *roaring.Bitmap) {
	it := deltaBitmap.Iterator()
	for it.HasNext() { // TODO : do something with file truncation during reading
		blockNo := it.Next()
		if pageReader.FileSize >= int64(blockNo+1)*int64(DatabasePageSize) { // whole block fits into file
			pageReader.Blocks = append(pageReader.Blocks, blockNo)
			tracelog.InfoLogger.Printf("Delta scan, block no: %d\n", blockNo)
		} else {
			break
		}
	}
}

// WriteDiffMapToHeader is currently used only with buffers, so we don't handle any writing errors
func (pageReader *IncrementalPageReader) WriteDiffMapToHeader(headerWriter io.Writer) {
	diffBlockCount := len(pageReader.Blocks)
	headerWriter.Write(utility.ToBytes(uint32(diffBlockCount)))

	for _, blockNo := range pageReader.Blocks {
		binary.Write(headerWriter, binary.LittleEndian, blockNo)
	}
	return
}

// SelectNewValidPage checks whether page is valid and if it so, then blockNo is appended to Blocks list
func (pageReader *IncrementalPageReader) SelectNewValidPage(pageBytes []byte, blockNo uint32) (valid bool) {
	pageHeader, _ := ParsePostgresPageHeader(bytes.NewReader(pageBytes))
	valid = pageHeader.IsValid()

	isNew := false

	if !valid {
		if pageHeader.IsNew() { // vacuumed page
			isNew = true
			valid = true
		} else {
			tracelog.WarningLogger.Println("Invalid page ", blockNo, " page header ", pageHeader)
			return false
		}
	}

	if isNew || (pageHeader.Lsn() >= pageReader.Lsn) {
		pageReader.Blocks = append(pageReader.Blocks, blockNo)
	}
	return
}


func (pageReader *IncrementalPageReader) SelectNewValidPage2(pageBytes []byte, blockNo uint32) (valid bool) {
	tracelog.InfoLogger.Printf("Full scan, block no: %d", blockNo)
	pageHeader, _ := ParsePostgresPageHeader(bytes.NewReader(pageBytes))
	tracelog.InfoLogger.Printf("Full scan, lsn: %d", pageReader.Lsn)
	tracelog.InfoLogger.Printf("Full scan, size: %d", pageReader.FileSize)
	io.Copy(os.Stderr, pageReader.PagedFile)
	valid = pageHeader.IsValid()

	isNew := false

	if !valid {
		if pageHeader.IsNew() { // vacuumed page
			isNew = true
			valid = true
		} else {
			tracelog.WarningLogger.Println("Invalid page ", blockNo, " page header ", pageHeader)
			return false
		}
	}

	if isNew || (pageHeader.Lsn() >= pageReader.Lsn) {
		pageReader.Blocks = append(pageReader.Blocks, blockNo)
	}
	return
}