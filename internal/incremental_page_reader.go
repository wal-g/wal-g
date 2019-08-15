package internal

import (
	"bytes"
	"encoding/binary"
	"github.com/RoaringBitmap/roaring"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/tracelog"
	"github.com/wal-g/wal-g/utility"
	"io"
	"strconv"
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
		for i, header := range headers {
			//found := false
			for _, block := range blocks {
				if block == pageReader2.Blocks[i] && header.Lsn() < START_LSN && header.Lsn() >= pageReader2.Lsn {
					tracelog.InfoLogger.Printf("Full scan, block no: %d\n", block)
					tracelog.InfoLogger.Printf("Full scan, lsn: %d\n", header.Lsn())
					tracelog.InfoLogger.Printf("Full scan, size: %d\n", pageReader.FileSize)
					tracelog.InfoLogger.Printf("diff block pdLsnH %d\n", header.pdLsnH)
					tracelog.InfoLogger.Printf("diff block pdLsnL           %d\n", header.pdLsnL     )
					tracelog.InfoLogger.Printf("diff block pdChecksum       %d\n", header.pdChecksum  )
					tracelog.InfoLogger.Printf("diff block pdFlags          %d\n", header.pdFlags       )
					tracelog.InfoLogger.Printf("diff block pdLower          %d\n", header.pdLower      )
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

		valid, _ := pageReader.SelectNewValidPage(pageBytes, currentBlockNumber) // TODO : torn page possibility
		if !valid {
			return NewInvalidBlockError(currentBlockNumber)
		}
	}
}

func (pageReader2 *IncrementalPageReader) PrintDiff(diff []uint32, pageReader1 *IncrementalPageReader) error {
	pageBytes := make([]byte, DatabasePageSize)
	if diff == nil || len(diff) == 0 {
		tracelog.InfoLogger.Println("Diff is empty")
		tracelog.InfoLogger.Println("lsn without diff: " + strconv.FormatUint(pageReader2.Lsn, 10))
		return nil
	}
	tracelog.InfoLogger.Println("Diff is not empty")
	tracelog.InfoLogger.Println("lsn: " + strconv.FormatUint(pageReader2.Lsn, 10))
	for currentBlockNumber := uint32(0); ; currentBlockNumber++ {
		found := false
		for i := 0; i < len(diff); i++ {
			if currentBlockNumber == diff[i] {
				found = true
				break
			}
		}

		_, err := io.ReadFull(pageReader1.PagedFile, pageBytes)

		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				tracelog.InfoLogger.Println("print diff eof")
				return nil
			}
			tracelog.InfoLogger.Printf("print diff err %v", err)
			return err
		}
		if !found {
			tracelog.InfoLogger.Printf("Block not found %d\n", currentBlockNumber)
			break
		}
		tracelog.InfoLogger.Printf("Block found %d\n", currentBlockNumber)

		//valid := pageReader1.SelectNewValidPage2(pageBytes, currentBlockNumber) // TODO : torn page possibility
		//if !valid {
		//	return NewInvalidBlockError(currentBlockNumber)
		//}
	}
	return nil
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
			return err, nil
		}

		valid, pageHeader := pageReader.SelectNewValidPage(pageBytes, currentBlockNumber) // TODO : torn page possibility
		if !valid {
			return NewInvalidBlockError(currentBlockNumber), nil
		} else {
			headers = append(headers, pageHeader)
		}
	}
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
	}
	return
}


func (pageReader *IncrementalPageReader) SelectNewValidPage2(pageBytes []byte, blockNo uint32, diff []uint32) (valid bool) {
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
		for _, diffBlockNo := range diff {
			if diffBlockNo == blockNo && pageHeader.Lsn() < START_LSN {
				tracelog.InfoLogger.Printf("Full scan, block no: %d\n", blockNo)
				tracelog.InfoLogger.Printf("Full scan, lsn: %d\n", pageHeader.Lsn())
				tracelog.InfoLogger.Printf("Full scan, size: %d\n", pageReader.FileSize)
				tracelog.InfoLogger.Printf("diff block pdLsnH %d\n", pageHeader.pdLsnH)
				tracelog.InfoLogger.Printf("diff block pdLsnL           %d\n", pageHeader.pdLsnL     )
				tracelog.InfoLogger.Printf("diff block pdChecksum       %d\n", pageHeader.pdChecksum  )
				tracelog.InfoLogger.Printf("diff block pdFlags          %d\n", pageHeader.pdFlags       )
				tracelog.InfoLogger.Printf("diff block pdLower          %d\n", pageHeader.pdLower      )
				tracelog.InfoLogger.Printf("diff block pdUpper          %d\n", pageHeader.pdUpper)
				tracelog.InfoLogger.Printf("diff block pdSpecial        %d\n", pageHeader.pdSpecial)
				tracelog.InfoLogger.Printf("diff block pdPageSizeVersion%d\n", pageHeader.pdPageSizeVersion)
				break
			}
		}
	}
	return
}

func (pageReader *IncrementalPageReader) FullScanInitialize3(diff []uint32) error {
	pageBytes := make([]byte, DatabasePageSize)
	for currentBlockNumber := uint32(0); ; currentBlockNumber++ {
		_, err := io.ReadFull(pageReader.PagedFile, pageBytes)

		if err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return nil
			}
			return err
		}

		valid := pageReader.SelectNewValidPage2(pageBytes, currentBlockNumber, diff) // TODO : torn page possibility
		if !valid {
			return NewInvalidBlockError(currentBlockNumber)
		}
	}
}