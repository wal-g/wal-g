package internal

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/RoaringBitmap/roaring"
	"github.com/tinsane/tracelog"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/utility"
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
		err, headers := pageReader2.FullScanInitialize2()
		if err != nil {
			tracelog.ErrorLogger.Fatalf("Keka3: %v", err)
		}
		tracelog.InfoLogger.Println("full init")
		blocks := diff(pageReader2.Blocks, pageReader1.Blocks)
		tracelog.InfoLogger.Printf("Length headers %d , blocks %d\n", len(headers), len(pageReader2.Blocks))
		for i, header := range headers {
			//found := false
			for _, block := range blocks {
				if block == pageReader2.Blocks[i] && header.Lsn() < START_LSN && header.Lsn() >= pageReader2.Lsn {
					tracelog.InfoLogger.Printf("Full scan, block no: %d\n", block)
					tracelog.InfoLogger.Printf("Full scan, lsn: %d\n", header.Lsn())
					tracelog.InfoLogger.Printf("Full scan, size: %d\n", pageReader.FileSize)
					tracelog.InfoLogger.Printf("diff block pdLsnH %d\n", header.pdLsnH)
					tracelog.InfoLogger.Printf("diff block pdLsnL           %d\n", header.pdLsnL)
					tracelog.InfoLogger.Printf("diff block pdChecksum       %d\n", header.pdChecksum)
					tracelog.InfoLogger.Printf("diff block pdFlags          %d\n", header.pdFlags)
					tracelog.InfoLogger.Printf("diff block pdLower          %d\n", header.pdLower)
					tracelog.InfoLogger.Printf("diff block pdUpper          %d\n", header.pdUpper)
					tracelog.InfoLogger.Printf("diff block pdSpecial        %d\n", header.pdSpecial)
					tracelog.InfoLogger.Printf("diff block pdPageSizeVersion%d\n", header.pdPageSizeVersion)
					//found = true
					break
				}
			}
			//if !found {
			//	tracelog.InfoLogger.Printf("Not found %d\n pageHeader", i)
			//}
		}
		tracelog.InfoLogger.Println("diff success")
		err = pageReader.FullScanInitialize()
		if err != nil {
			tracelog.ErrorLogger.Fatalf("Keka4: %v", err)
		}

	}

	pageReader.WriteDiffMapToHeader(&headerBuffer)
	pageReader.Next = headerBuffer.Bytes()
	pageDataSize := int64(len(pageReader.Blocks)) * int64(DatabasePageSize)
	size = int64(headerBuffer.Len()) + pageDataSize
	return
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

		valid, _ := pageReader.SelectNewValidPage(pageBytes, currentBlockNumber) // TODO : torn page possibility
		if !valid {
			return NewInvalidBlockError(currentBlockNumber)
		}
	}
}

// WriteDiffMapToHeader is currently used only with buffers, so we don't handle any writing errors
func (pageReader *IncrementalPageReader) WriteDiffMapToHeader(headerWriter io.Writer) {
	diffBlockCount := len(pageReader.Blocks)
	_, _ = headerWriter.Write(utility.ToBytes(uint32(diffBlockCount)))

	for _, blockNo := range pageReader.Blocks {
		_ = binary.Write(headerWriter, binary.LittleEndian, blockNo)
	}
	return
}

func (pageReader *IncrementalPageReader) DeltaBitmapInitialize2(deltaBitmap *roaring.Bitmap) {
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

func (pageReader *IncrementalPageReader) FullScanInitialize2() (error, []*PostgresPageHeader) {
	pageBytes := make([]byte, DatabasePageSize)
	headers := make([]*PostgresPageHeader, 0)
	for currentBlockNumber := uint32(0); ; currentBlockNumber++ {
		_, err := io.ReadFull(pageReader.PagedFile, pageBytes)

		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil, headers
			}
			return err, headers
		}

		valid, pageHeader := pageReader.SelectNewValidPage(pageBytes, currentBlockNumber) // TODO : torn page possibility
		if !valid {
			return NewInvalidBlockError(currentBlockNumber), headers
		} else if pageHeader != nil {
			headers = append(headers, pageHeader)
		}
	}
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

// SelectNewValidPage checks whether page is valid and if it so, then blockNo is appended to Blocks list
func (pageReader *IncrementalPageReader) SelectNewValidPage(pageBytes []byte, blockNo uint32) (valid bool, pageHeader *PostgresPageHeader) {
	pageHeader, _ = ParsePostgresPageHeader(bytes.NewReader(pageBytes))
	valid = pageHeader.IsValid()
	isNew := false

	if !valid {
		if pageHeader.IsNew() { // vacuumed page
			isNew = true
			valid = true
		} else {
			tracelog.WarningLogger.Println("Invalid page ", blockNo, " page header ", pageHeader)
			return false, nil
		}
	}

	if isNew || (pageHeader.Lsn() >= pageReader.Lsn) {
		pageReader.Blocks = append(pageReader.Blocks, blockNo)
		return valid, pageHeader
	} else {
		return valid, nil
	}
}
