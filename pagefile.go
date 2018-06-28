//
// This file provides low level routines for handling incremental backup
// Incremental file format is:
// 4 bytes header with designation information, format version and magic number
// 8 bytes uint file size
// 4 bytes uint changed pages count N
// (N * 4) bytes for Block Numbers of changed pages
// (N * PostgresPageSize) bytes for changed page data
//

package walg

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
)

const (
	PostgresPageSize     uint16 = 8192
	sizeofInt32                 = 4
	sizeofInt16                 = 2
	sizeofInt64                 = 8
	signatureMagicNumber byte   = 0x55
	invalidLsn           uint64 = 0
	validFlags                  = 7
	layoutVersion               = 4
	headerSize                  = 24
)

// ParsePageHeader reads information from PostgreSQL page header. Exported for test reasons.
func ParsePageHeader(data []byte) (lsn uint64, valid bool) {
	// Any ideas on how to make this code pretty and nice?
	le := binary.LittleEndian
	pdLsnH := le.Uint32(data[0:sizeofInt32])
	pdLsnL := le.Uint32(data[sizeofInt32 : 2*sizeofInt32])

	// pd_checksum := binary.LittleEndian.Uint16(data[2*sizeofInt32:2*sizeofInt32+sizeofInt16])
	pdFlags := le.Uint16(data[2*sizeofInt32+sizeofInt16 : 2*sizeofInt32+2*sizeofInt16])
	pdLower := le.Uint16(data[2*sizeofInt32+2*sizeofInt16 : 2*sizeofInt32+3*sizeofInt16])
	pdUpper := le.Uint16(data[2*sizeofInt32+3*sizeofInt16 : 2*sizeofInt32+4*sizeofInt16])
	pdSpecial := le.Uint16(data[2*sizeofInt32+4*sizeofInt16 : 2*sizeofInt32+5*sizeofInt16])
	pdPagesizeVersion := le.Uint16(data[2*sizeofInt32+5*sizeofInt16 : 2*sizeofInt32+6*sizeofInt16])

	lsn = ((uint64(pdLsnH)) << 32) + uint64(pdLsnL)
	if (pdFlags&validFlags) != pdFlags ||
		pdLower < headerSize ||
		pdLower > pdUpper ||
		pdUpper > pdSpecial ||
		pdSpecial > PostgresPageSize ||
		(lsn == invalidLsn) ||
		pdPagesizeVersion != PostgresPageSize+layoutVersion {
		valid = false
	} else {
		valid = true
	}

	return
}

// IsPagedFile checks basic expectations for paged file
func IsPagedFile(info os.FileInfo, fileName string) bool {

	// For details on which file is paged see
	// https://www.postgresql.org/message-id/flat/F0627DEB-7D0D-429B-97A9-D321450365B4%40yandex-team.ru#F0627DEB-7D0D-429B-97A9-D321450365B4@yandex-team.ru
	if info.IsDir() ||
		strings.HasSuffix(fileName, "_fsm") ||
		strings.HasSuffix(fileName, "_vm") ||
		((!strings.Contains(fileName, "base")) && (!strings.Contains(fileName, "global")) && (!strings.Contains(fileName, "pg_tblspc"))) ||
		info.Size() == 0 ||
		info.Size()%int64(PostgresPageSize) != 0 {
		return false
	}
	return true
}

// IncrementalPageReader constructs difference map during initialization and than re-read file
// Diff map can be of 1Gb/PostgresBlockSize elements == 512Kb
type IncrementalPageReader struct {
	backlog chan []byte
	file    *io.LimitedReader
	seeker  io.Seeker
	closer  io.Closer
	info    os.FileInfo
	lsn     uint64
	next    *[]byte
	blocks  []uint32
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
		bytes := (*(pageReader.next))[n:]
		pageReader.next = &(bytes)
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
	pageBytes := make([]byte, PostgresPageSize)
	blockNo := pageReader.blocks[0]
	pageReader.blocks = pageReader.blocks[1:]
	offset := int64(blockNo) * int64(PostgresPageSize)
	_, err := pageReader.seeker.Seek(offset, 0)
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
	return pageReader.closer.Close()
}

// ErrInvalidBlock indicates that file contain invalid page and cannot be archived incrementally
var ErrInvalidBlock = errors.New("Block is not valid")

func (pageReader *IncrementalPageReader) initialize() (size int64, err error) {
	size = 0
	// "wi" at the head stands for "wal-g increment"
	// format version "1", signature magic number
	pageReader.next = &[]byte{'w', 'i', '1', signatureMagicNumber}
	size += sizeofInt32
	fileSizeBytes := make([]byte, sizeofInt64)
	fileSize := pageReader.info.Size()
	binary.LittleEndian.PutUint64(fileSizeBytes, uint64(fileSize))
	pageReader.backlog <- fileSizeBytes
	size += sizeofInt64

	pageBytes := make([]byte, PostgresPageSize)
	pageReader.blocks = make([]uint32, 0, fileSize/int64(PostgresPageSize))

	for currentBlockNumber := uint32(0); ; currentBlockNumber++ {
		n, err := io.ReadFull(pageReader.file, pageBytes)
		if err == io.ErrUnexpectedEOF || n%int(PostgresPageSize) != 0 {
			return 0, errors.New("Unexpected EOF during increment scan")
		}

		if err == io.EOF {
			diffBlockCount := len(pageReader.blocks)
			lenBytes := make([]byte, sizeofInt32)
			binary.LittleEndian.PutUint32(lenBytes, uint32(diffBlockCount))
			pageReader.backlog <- lenBytes
			size += sizeofInt32

			diffMap := make([]byte, diffBlockCount*sizeofInt32)

			for i, blockNum := range pageReader.blocks {
				binary.LittleEndian.PutUint32(diffMap[i*sizeofInt32:(i+1)*sizeofInt32], blockNum)
			}

			pageReader.backlog <- diffMap
			size += int64(diffBlockCount * sizeofInt32)
			dataSize := int64(len(pageReader.blocks)) * int64(PostgresPageSize)
			size += dataSize
			_, err := pageReader.seeker.Seek(0, 0)
			if err != nil {
				return 0, nil
			}
			pageReader.file.N = dataSize
			return size, nil
		}

		if err != nil {
			return 0, err
		}

		lsn, valid := ParsePageHeader(pageBytes)

		allZeroes := false

		if !valid {
			if allZero(pageBytes) {
				allZeroes = true
			} else {
				return 0, ErrInvalidBlock
			}
		}

		if (allZeroes) || (lsn >= pageReader.lsn) {
			pageReader.blocks = append(pageReader.blocks, currentBlockNumber)
		}
	}
}

// ReadDatabaseFile tries to read file as an incremental data file if possible, otherwise just open the file
func ReadDatabaseFile(fileName string, lsn *uint64, isNew bool) (io.ReadCloser, bool, int64, error) {
	info, err := os.Stat(fileName)
	fileSize := info.Size()
	if err != nil {
		return nil, false, fileSize, err
	}

	file, err := os.Open(fileName)
	if err != nil {
		return nil, false, fileSize, err
	}

	if lsn == nil || isNew || !IsPagedFile(info, fileName) {
		return file, false, fileSize, nil
	}

	lim := &io.LimitedReader{
		R: io.MultiReader(file, &ZeroReader{}),
		N: int64(fileSize),
	}

	reader := &IncrementalPageReader{make(chan []byte, 4), lim, file, file, info, *lsn, nil, nil}
	incrSize, err := reader.initialize()
	if err != nil {
		if err == ErrInvalidBlock {
			file.Close()
			fmt.Printf("File %v has invalid pages, fallback to full backup\n", fileName)
			file, err = os.Open(fileName)
			if err != nil {
				return nil, false, fileSize, err
			}
			return file, false, fileSize, nil
		}

		return nil, false, fileSize, err
	}
	return reader, true, incrSize, nil
}

// ApplyFileIncrement changes pages according to supplied change map file
func ApplyFileIncrement(fileName string, increment io.Reader) error {
	fmt.Println("Incrementing " + fileName)
	header := make([]byte, sizeofInt32)
	fileSizeBytes := make([]byte, sizeofInt64)
	diffBlockBytes := make([]byte, sizeofInt32)

	_, err := io.ReadFull(increment, header)
	if err != nil {
		return err
	}

	if header[0] != 'w' || header[1] != 'i' || header[3] != signatureMagicNumber {
		return errors.New("Invalid increment file header")
	}
	if header[2] != '1' {
		return errors.New("Unknown increment file header")
	}

	_, err = io.ReadFull(increment, fileSizeBytes)
	if err != nil {
		return err
	}
	_, err = io.ReadFull(increment, diffBlockBytes)
	if err != nil {
		return err
	}

	fileSize := binary.LittleEndian.Uint64(fileSizeBytes)
	diffBlockCount := binary.LittleEndian.Uint32(diffBlockBytes)
	diffMap := make([]byte, diffBlockCount*sizeofInt32)

	_, err = io.ReadFull(increment, diffMap)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(fileName, os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer file.Close()
	defer file.Sync()

	err = file.Truncate(int64(fileSize))
	if err != nil {
		return err
	}

	page := make([]byte, PostgresPageSize)
	for i := uint32(0); i < diffBlockCount; i++ {
		blockNo := binary.LittleEndian.Uint32(diffMap[i*sizeofInt32 : (i+1)*sizeofInt32])
		_, err = io.ReadFull(increment, page)
		if err != nil {
			return err
		}

		_, err = file.WriteAt(page, int64(blockNo)*int64(PostgresPageSize))
		if err != nil {
			return err
		}
	}

	all, _ := increment.Read(make([]byte, 1))
	if all > 0 {
		return errors.New("Expected end of Tar")
	}

	return nil
}

func allZero(s []byte) bool {
	for _, v := range s {
		if v != 0 {
			return false
		}
	}
	return true
}
