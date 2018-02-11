//
// This file provides low level routines for handling incremental backup
// Incremental file format is:
// 4 bytes header with designation information, format version and magic number
// 8 bytes uint file size
// 4 bytes uint changed pages count N
// (N * 4) bytes for Block Numbers of changed pages
// (N * BlockSize) bytes for changed page data
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
	BlockSize            uint16 = 8192
	sizeofInt32                 = 4
	sizeofInt16                 = 2
	sizeofInt64                 = 8
	sizeofPgPageHeader          = 24
	signatureMagicNumber byte   = 0x55
	invalid_lsn          uint64 = 0
	valid_flags                 = 7
	layout_version              = 4
	header_size                 = 24
)

func ParsePageHeader(data []byte) (lsn uint64, valid bool) {
	// Any ideas on how to make this code pretty and nice?
	le := binary.LittleEndian
	pd_lsn_h := le.Uint32(data[0:sizeofInt32])
	pd_lsn_l := le.Uint32(data[sizeofInt32 : 2*sizeofInt32])

	// pd_checksum := binary.LittleEndian.Uint16(data[2*sizeofInt32:2*sizeofInt32+sizeofInt16])
	pd_flags := le.Uint16(data[2*sizeofInt32+sizeofInt16 : 2*sizeofInt32+2*sizeofInt16])
	pd_lower := le.Uint16(data[2*sizeofInt32+2*sizeofInt16 : 2*sizeofInt32+3*sizeofInt16])
	pd_upper := le.Uint16(data[2*sizeofInt32+3*sizeofInt16 : 2*sizeofInt32+4*sizeofInt16])
	pd_special := le.Uint16(data[2*sizeofInt32+4*sizeofInt16 : 2*sizeofInt32+5*sizeofInt16])
	pd_pagesize_version := le.Uint16(data[2*sizeofInt32+5*sizeofInt16 : 2*sizeofInt32+6*sizeofInt16])

	lsn = ((uint64(pd_lsn_h)) << 32) + uint64(pd_lsn_l)
	if (pd_flags&valid_flags) != pd_flags ||
		pd_lower < header_size ||
		pd_lower > pd_upper ||
		pd_upper > pd_special ||
		pd_special > BlockSize ||
		(lsn == invalid_lsn) ||
		pd_pagesize_version != BlockSize+layout_version {
		valid = false
	} else {
		valid = true
	}

	return
}

func IsPagedFile(info os.FileInfo, fileName string) bool {

	// For details on which file is paged see
	// https://www.postgresql.org/message-id/flat/F0627DEB-7D0D-429B-97A9-D321450365B4%40yandex-team.ru#F0627DEB-7D0D-429B-97A9-D321450365B4@yandex-team.ru
	if info.IsDir() ||
		strings.HasSuffix(fileName, "_fsm") ||
		strings.HasSuffix(fileName, "_vm") ||
		((!strings.Contains(fileName, "base")) && (!strings.Contains(fileName, "global")) && (!strings.Contains(fileName, "pg_tblspc"))) ||
		info.Size() == 0 ||
		info.Size()%int64(BlockSize) != 0 {
		return false
	}
	return true
}

// Reader consturcts difference map during initialization and than re-read file
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

func (pr *IncrementalPageReader) Read(p []byte) (n int, err error) {
	err = nil
	if pr.next == nil {
		return 0, io.EOF
	}
	n = copy(p, *pr.next)
	if n == len(*pr.next) {
		pr.next = nil
	} else {
		bytes := (*(pr.next))[n:]
		pr.next = &(bytes)
	}

	if pr.next == nil {
		err = pr.DrainMoreData()
	}

	return n, err
}
func (pr *IncrementalPageReader) DrainMoreData() error {
	for len(pr.blocks) > 0 && len(pr.backlog) < 2 {
		err := pr.AdvanceFileReader()
		if err != nil {
			return err
		}
	}

	if len(pr.backlog) > 0 {
		moreBytes := <-pr.backlog
		pr.next = &moreBytes
	}

	return nil
}

func (pr *IncrementalPageReader) AdvanceFileReader() error {
	pageBytes := make([]byte, BlockSize)
	blockNo := pr.blocks[0]
	pr.blocks = pr.blocks[1:]
	offset := int64(blockNo) * int64(BlockSize)
	_, err := pr.seeker.Seek(offset, 0)
	if err != nil {
		return err
	}
	_, err = io.ReadFull(pr.file, pageBytes)
	if err == nil {
		pr.backlog <- pageBytes
	}
	return err
}

func (pr *IncrementalPageReader) Close() error {
	return pr.closer.Close()
}

var InvalidBlock = errors.New("Block is not valid")

func (pr *IncrementalPageReader) Initialize() (size int64, err error) {
	size = 0
	// "wi" at the head stands for "wal-g increment"
	// format version "1", signature magic number
	pr.next = &[]byte{'w', 'i', '1', signatureMagicNumber}
	size += sizeofInt32
	fileSizeBytes := make([]byte, sizeofInt64)
	fileSize := pr.info.Size()
	binary.LittleEndian.PutUint64(fileSizeBytes, uint64(fileSize))
	pr.backlog <- fileSizeBytes
	size += sizeofInt64

	pageBytes := make([]byte, BlockSize)
	pr.blocks = make([]uint32, 0, fileSize/int64(BlockSize))

	for currentBlockNumber := uint32(0); ; currentBlockNumber++ {
		n, err := io.ReadFull(pr.file, pageBytes)
		if err == io.ErrUnexpectedEOF || n%int(BlockSize) != 0 {
			return 0, errors.New("Unexpected EOF during increment scan")
		}

		if err == io.EOF {
			diffBlockCount := len(pr.blocks)
			lenBytes := make([]byte, sizeofInt32)
			binary.LittleEndian.PutUint32(lenBytes, uint32(diffBlockCount))
			pr.backlog <- lenBytes
			size += sizeofInt32

			diffMap := make([]byte, diffBlockCount*sizeofInt32)

			for index, blockNo := range pr.blocks {
				binary.LittleEndian.PutUint32(diffMap[index*sizeofInt32:(index+1)*sizeofInt32], blockNo)
			}

			pr.backlog <- diffMap
			size += int64(diffBlockCount * sizeofInt32)
			dataSize := int64(len(pr.blocks)) * int64(BlockSize)
			size += dataSize
			_, err := pr.seeker.Seek(0, 0)
			if err != nil {
				return 0, nil
			}
			pr.file.N = dataSize
			return size, nil
		}

		if err == nil {
			lsn, valid := ParsePageHeader(pageBytes)

			var allZeroes = false
			if !valid && allZero(pageBytes) {
				allZeroes = true
				valid = true
			}

			if !valid {
				return 0, InvalidBlock
			}

			if (allZeroes) || (lsn >= pr.lsn) {
				pr.blocks = append(pr.blocks, currentBlockNumber)
			}
		} else {
			return 0, err
		}
	}
}

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
	incrSize, err := reader.Initialize()
	if err != nil {
		if err == InvalidBlock {
			file.Close()
			fmt.Printf("File %v has invalid pages, fallback to full backup\n", fileName)
			file, err = os.Open(fileName)
			if err != nil {
				return nil, false, fileSize, err
			}
			return file, false, fileSize, nil
		} else {
			return nil, false, fileSize, err
		}
	}
	return reader, true, incrSize, nil
}

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
	defer file.Close()
	defer file.Sync()
	if err != nil {
		return err
	}

	err = file.Truncate(int64(fileSize))
	if err != nil {
		return err
	}

	page := make([]byte, BlockSize)
	for i := uint32(0); i < diffBlockCount; i++ {
		blockNo := binary.LittleEndian.Uint32(diffMap[i*sizeofInt32 : (i+1)*sizeofInt32])
		_, err = io.ReadFull(increment, page)
		if err != nil {
			return err
		}

		_, err = file.WriteAt(page, int64(blockNo)*int64(BlockSize))
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
