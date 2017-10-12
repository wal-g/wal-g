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

/*
// This block is CGo Postgres page header parsing. This is comment in comment. Cool.
#include <inttypes.h>
typedef struct PageHeaderData
{
	uint32_t 		pd_lsn_h;
	uint32_t 		pd_lsn_l;

	uint16_t		pd_checksum;
	uint16_t		pd_flags;
	uint16_t		pd_lower;
	uint16_t 		pd_upper;
	uint16_t 		pd_special;
	uint16_t		pd_pagesize_version;
	uint32_t 		pd_prune_xid;
} PageHeaderData;

typedef struct PageProbeResult
{
	int success;
	uint64_t lsn;
} PageProbeResult;

#define valid_flags     (7)
#define invalid_lsn     (0)
#define layout_version  (4)
#define header_size     (24)
#define block_size		(8192)

PageProbeResult GetLSNIfPageIsValid(void* ptr)
{
	PageHeaderData* data = (PageHeaderData*) ptr;
	PageProbeResult result = {0 , invalid_lsn};

	//LSN layout is neither big endian nor low endiang, here we conver it to comparable form
	// This form must be coherent with ParseLsn() function which is used by StartBackup()
	result.lsn = (((uint64_t)data->pd_lsn_h) << 32) + ((uint64_t)data->pd_lsn_l);

	if ((data->pd_flags & valid_flags) != data->pd_flags ||
		data->pd_lower < header_size ||
		data->pd_lower > data->pd_upper ||
		data->pd_upper > data->pd_special ||
		data->pd_special > block_size ||
		(result.lsn == invalid_lsn)||
		data->pd_pagesize_version != block_size + layout_version)
	{
		return result;
	}

	result.success = 1;
	return result;
}
*/
import "C"
import (
	"unsafe"
	"os"
	"strings"
	"io"
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	BlockSize            uint16 = 8192
	sizeofInt32                 = 4
	sizeofInt64                 = 8
	sizeofPgPageHeader          = 24
	signatureMagicNumber byte   = 0x55
)

func ParsePageHeader(data []byte) (uint64, bool) {
	res := C.GetLSNIfPageIsValid(unsafe.Pointer(&data[0]))

	// this mess is caused by STDC _Bool
	if res.success != 0 {
		return uint64(res.lsn), true
	}
	return uint64(res.lsn), false
}

func IsPagedFile(info os.FileInfo) bool {
	StaticStructAllignmentCheck()
	name := info.Name()

	// For details on which file is paged see
	// https://www.postgresql.org/message-id/flat/F0627DEB-7D0D-429B-97A9-D321450365B4%40yandex-team.ru#F0627DEB-7D0D-429B-97A9-D321450365B4@yandex-team.ru
	if info.IsDir() ||
		strings.HasSuffix(name, "_fsm") ||
		info.Size() == 0 ||
		info.Size()%int64(BlockSize) != 0 {
		return false
	}
	return true
}

// This function ensures Postgres page header sitructure has correct size
func StaticStructAllignmentCheck() {
	var dummy C.PageHeaderData
	sizeof := unsafe.Sizeof(dummy)
	if sizeof != sizeofPgPageHeader {
		panic("Error in PageHeaderData struct compilation");
	}
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
	pr.next = &[]byte{'w', 'i', '1', signatureMagicNumber};
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

	if lsn == nil || isNew || !IsPagedFile(info) {
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

func ApplyFileIncrement(fileName string, increment io.Reader) (error) {
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
		blockNo := binary.LittleEndian.Uint32(diffMap[i*sizeofInt32:(i+1)*sizeofInt32])
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
