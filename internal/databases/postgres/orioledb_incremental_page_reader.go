package postgres

import (
	"bytes"
	"encoding/binary"
	"io"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/internal/walparser/parsingutil"
	"github.com/wal-g/wal-g/utility"
)

const CompressedPageSize = 512

// IncrementFileHeader contains "wi" at the head which stands for "wal-g increment"
// format version "1", signature magic number
// var IncrementFileHeader = []byte{'w', 'i', '1', SignatureMagicNumber}

// OrioledbIncrementalPageReader constructs difference map during initialization and than re-read file
// Diff map may consist of 1Gb/PostgresBlockSize elements == 512Kb
type OrioledbIncrementalPageReader struct {
	PagedFile  ioextensions.ReadSeekCloser
	FileSize   int64
	Compressed bool
	ChkpNum    uint32
	Next       []byte
	Blocks     []uint32
}

func (pageReader *OrioledbIncrementalPageReader) Read(p []byte) (n int, err error) {
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

func (pageReader *OrioledbIncrementalPageReader) DrainMoreData() (succeed bool, err error) {
	if len(pageReader.Blocks) == 0 {
		return false, nil
	}
	err = pageReader.AdvanceFileReader()
	if err != nil {
		return false, err
	}
	return true, nil
}

func (pageReader *OrioledbIncrementalPageReader) AdvanceFileReader() error {
	pageSize := DatabasePageSize
	if pageReader.Compressed {
		pageSize = CompressedPageSize
	}
	pageBytes := make([]byte, pageSize)
	blockNo := pageReader.Blocks[0]
	pageReader.Blocks = pageReader.Blocks[1:]
	offset := int64(blockNo) * pageSize
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

// Close OrioledbIncrementalPageReader
func (pageReader *OrioledbIncrementalPageReader) Close() error {
	return pageReader.PagedFile.Close()
}

// TODO : unit tests
// TODO : "initialize" is rather meaningless name, maybe this func should be decomposed
func (pageReader *OrioledbIncrementalPageReader) initialize(deltaBitmap *roaring.Bitmap) (size int64, err error) {
	var headerBuffer bytes.Buffer
	headerBuffer.Write(IncrementFileHeader)
	fileSize := pageReader.FileSize
	headerBuffer.Write(utility.ToBytes(uint64(fileSize)))
	pageReader.Compressed = fileSize%DatabasePageSize != 0
	pageSize := DatabasePageSize
	if pageReader.Compressed {
		pageSize = CompressedPageSize
	}
	headerBuffer.Write(utility.ToBytes(uint16(pageSize)))
	pageReader.Blocks = make([]uint32, 0, fileSize/pageSize)

	if deltaBitmap == nil {
		err := pageReader.FullScanInitialize()
		if err != nil {
			return 0, err
		}
	} else {
		pageReader.DeltaBitmapInitialize(deltaBitmap)
	}

	pageReader.WriteDiffMapToHeader(&headerBuffer)
	pageReader.Next = headerBuffer.Bytes()
	pageDataSize := int64(len(pageReader.Blocks)) * pageSize
	size = int64(headerBuffer.Len()) + pageDataSize
	return
}

func (pageReader *OrioledbIncrementalPageReader) DeltaBitmapInitialize(deltaBitmap *roaring.Bitmap) {
	it := deltaBitmap.Iterator()
	pageSize := DatabasePageSize
	if pageReader.Compressed {
		pageSize = CompressedPageSize
	}
	for it.HasNext() { // TODO : do something with file truncation during reading
		blockNo := it.Next()
		if pageReader.FileSize >= int64(blockNo+1)*pageSize { // whole block fits into file
			pageReader.Blocks = append(pageReader.Blocks, blockNo)
		} else {
			break
		}
	}
}

func (pageReader *OrioledbIncrementalPageReader) FullScanInitialize() error {
	if !pageReader.Compressed {
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
				return newInvalidBlockError(currentBlockNumber)
			}
		}
	} else {
		readBytes := uint32(0)
		const sizeOfHeader = 8
		for {
			header := make([]byte, sizeOfHeader)

			_, err := io.ReadFull(pageReader.PagedFile, header)

			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					return nil
				}
				return err
			}
			pageSize := binary.LittleEndian.Uint16(header)
			// because pageSize is aligned to 4 bytes
			chkpNum := binary.LittleEndian.Uint32(header[4:])
			blocksTotal := (pageSize + sizeOfHeader + CompressedPageSize - 1) / CompressedPageSize
			full_size := blocksTotal*CompressedPageSize - sizeOfHeader

			pageBytes := make([]byte, full_size)
			_, err = io.ReadFull(pageReader.PagedFile, pageBytes)

			if err != nil {
				if err == io.EOF || err == io.ErrUnexpectedEOF {
					return nil
				}
				return err
			}

			if chkpNum >= pageReader.ChkpNum {
				currentBlockNumber := readBytes / CompressedPageSize
				for blockNumber := uint32(0); blockNumber < uint32(blocksTotal); blockNumber++ {
					pageReader.Blocks = append(pageReader.Blocks, currentBlockNumber+blockNumber)
				}
			} else {
				return newInvalidBlockError(readBytes / CompressedPageSize)
			}
			readBytes = readBytes + sizeOfHeader + uint32(full_size)
		}
	}
}

// WriteDiffMapToHeader is currently used only with buffers, so we don't handle any writing errors
func (pageReader *OrioledbIncrementalPageReader) WriteDiffMapToHeader(headerWriter io.Writer) {
	diffBlockCount := len(pageReader.Blocks)
	_, _ = headerWriter.Write(utility.ToBytes(uint32(diffBlockCount)))

	for _, blockNo := range pageReader.Blocks {
		_ = binary.Write(headerWriter, binary.LittleEndian, blockNo)
	}
}

//  typedef struct
//  {
// 	 pg_atomic_uint32 state;
// 	 pg_atomic_uint32 usageCount;
// 	 uint32		pageChangeCount;
//  } OrioleDBPageHeader;

// typedef struct
// {
// 	OrioleDBPageHeader o_header;
// 	uint32		flags:6,
// 				field1:11,
// 				field2:15;

// 	UndoLocation undoLocation;
// 	CommitSeqNo csn;

// 	uint64		rightLink;
// 	uint32		checkpointNum;
// 	LocationIndex maxKeyLen;
// 	OffsetNumber prevInsertOffset;
// 	OffsetNumber chunksCount;
// 	OffsetNumber itemsCount;
// 	OffsetNumber hikeysEnd;
// 	LocationIndex dataSize;
// 	BTreePageChunkDesc chunkDesc[1];
// } BTreePageHeader;

// typedef struct
// {
// 	uint32		shortLocation:12,
// 				offset:10,
// 				hikeyShortLocation:8,
// 				hikeyFlags:2;
// } BTreePageChunkDesc;

type OrioledbPageHeader struct {
	state               uint32
	usageCount          uint32
	pageChangeCount     uint32
	flags_field1_field2 uint32
	undoLocation        uint64
	csn                 uint64
	rightLink           uint64
	checkpointNum       uint32
	maxKeyLen           uint16
	prevInsertOffset    uint16
	chunksCount         uint16
	itemsCount          uint16
	hikeysEnd           uint16
	dataSize            uint16
	// chunkDesc           []uint32
}

func (header *OrioledbPageHeader) isValid() bool {
	// TODO: Add page validation
	return true
}

// parseOrioledbPageHeader reads information from PostgreSQL page header. Exported for test reasons.
func parseOrioledbPageHeader(reader io.Reader) (*OrioledbPageHeader, error) {
	pageHeader := OrioledbPageHeader{}
	fields := []parsingutil.FieldToParse{
		{Field: &pageHeader.state, Name: "state"},
		{Field: &pageHeader.usageCount, Name: "usageCount"},
		{Field: &pageHeader.pageChangeCount, Name: "pageChangeCount"},

		{Field: &pageHeader.flags_field1_field2, Name: "flags_field1_field2"},
		{Field: &pageHeader.undoLocation, Name: "undoLocation"},
		{Field: &pageHeader.csn, Name: "csn"},
		{Field: &pageHeader.rightLink, Name: "rightLink"},
		{Field: &pageHeader.checkpointNum, Name: "checkpointNum"},
		{Field: &pageHeader.maxKeyLen, Name: "maxKeyLen"},
		{Field: &pageHeader.prevInsertOffset, Name: "prevInsertOffset"},
		{Field: &pageHeader.chunksCount, Name: "chunksCount"},
		{Field: &pageHeader.itemsCount, Name: "itemsCount"},
		{Field: &pageHeader.hikeysEnd, Name: "hikeysEnd"},
		{Field: &pageHeader.dataSize, Name: "dataSize"},
		// {Field: &pageHeader.chunkDesc, Name: "chunkDesc"},
	}
	err := parsingutil.ParseMultipleFieldsFromReader(fields, reader)
	if err != nil {
		return nil, err
	}

	return &pageHeader, nil
}

// SelectNewValidPage checks whether page is valid and if it so, then blockNo is appended to Blocks list
func (pageReader *OrioledbIncrementalPageReader) SelectNewValidPage(pageBytes []byte, blockNo uint32) (valid bool) {
	pageHeader, _ := parseOrioledbPageHeader(bytes.NewReader(pageBytes))
	valid = pageHeader.isValid()

	if !valid {
		tracelog.DebugLogger.Println("Invalid page ", blockNo, " page header ", pageHeader)
		return false
	}

	if pageHeader.checkpointNum >= pageReader.ChkpNum {
		pageReader.Blocks = append(pageReader.Blocks, blockNo)
	}
	return
}

func isOrioledbDataPath(filePath string) bool {
	if !strings.Contains(filePath, "orioledb_data") ||
		!pagedFilenameRegexp.MatchString(path.Base(filePath)) {
		return false
	}
	return true
}

func isOrioledbDataFile(info os.FileInfo, filePath string) bool {
	if info.IsDir() ||
		info.Size() == 0 ||
		!isOrioledbDataPath(filePath) {
		return false
	}
	return true
}

func OrioledbReadIncrementalFile(filePath string,
	fileSize int64,
	chkpNum uint32,
	deltaBitmap *roaring.Bitmap) (fileReader io.ReadCloser, size int64, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, err
	}

	fileReadSeekCloser := &ioextensions.ReadSeekCloserImpl{
		Reader: limiters.NewDiskLimitReader(file),
		Seeker: file,
		Closer: file,
	}

	pageReader := &OrioledbIncrementalPageReader{fileReadSeekCloser, fileSize, false, chkpNum, nil, nil}
	incrementSize, err := pageReader.initialize(deltaBitmap)
	if err != nil {
		utility.LoggedClose(file, "")
		return nil, 0, err
	}
	return pageReader, incrementSize, nil
}

func OrioledbSetStartChkpNum(bh *BackupHandler) {
	OrioledbDataDirectory := bh.PgInfo.PgDataDirectory + "/orioledb_data"
	xidRegEx, err := regexp.Compile(`^.+\.(xid)$`)
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Cannot compile xid regex")
	}

	chkpNum := uint32(0)
	err = filepath.Walk(OrioledbDataDirectory, func(path string, info os.FileInfo, err error) error {
		if err == nil && xidRegEx.MatchString(info.Name()) {
			xid := strings.Split(info.Name(), ".")[0]
			tempChkpNum, err := strconv.Atoi(xid)
			if err != nil {
				tracelog.ErrorLogger.Fatalf("Cannot parse chkpNum: %s", xid)
			}
			chkpNum = uint32(tempChkpNum)
			return filepath.SkipAll
		}
		if info.IsDir() && filepath.Base(info.Name()) != "orioledb_data" {
			return filepath.SkipDir
		} else {
			return nil
		}
	})
	if err != nil {
		tracelog.ErrorLogger.Fatalf("Cannot find any xid file")
	}

	bh.CurBackupInfo.startChkpNum = &chkpNum
}
