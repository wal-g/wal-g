//
// This file provides low level routines for handling incremental backup
// Incremental file format is:
// 4 bytes header with designation information, format version and magic number
// 8 bytes uint file size
// 4 bytes uint changed pages count N
// (N * 4) bytes for Block Numbers of changed pages
// (N * WalPageSize) bytes for changed page data
//

package walg

import (
	"encoding/binary"
	"fmt"
	"github.com/pkg/errors"
	"io"
	"os"
	"strings"
)

const (
	WalPageSize          uint16 = 8192
	sizeofInt32                 = 4
	sizeofInt16                 = 2
	sizeofInt64                 = 8
	signatureMagicNumber byte   = 0x55
	invalidLsn           uint64 = 0
	validFlags                  = 7
	layoutVersion               = 4
	headerSize                  = 24
)

// InvalidBlockError indicates that file contain invalid page and cannot be archived incrementally
var InvalidBlockError = errors.New("block is invalid")

var InvalidIncrementFileHeaderError = errors.New("Invalid increment file header")
var UnknownIncrementFileHeaderError = errors.New("Unknown increment file header")
var UnexpectedTarDataError = errors.New("Expected end of Tar")

// IsPagedFile checks basic expectations for paged file
func IsPagedFile(info os.FileInfo, fileName string) bool {

	// For details on which file is paged see
	// https://www.postgresql.org/message-id/flat/F0627DEB-7D0D-429B-97A9-D321450365B4%40yandex-team.ru#F0627DEB-7D0D-429B-97A9-D321450365B4@yandex-team.ru
	if info.IsDir() ||
		strings.HasSuffix(fileName, "_fsm") ||
		strings.HasSuffix(fileName, "_vm") ||
		((!strings.Contains(fileName, "base")) && (!strings.Contains(fileName, "global")) && (!strings.Contains(fileName, "pg_tblspc"))) ||
		info.Size() == 0 ||
		info.Size()%int64(WalPageSize) != 0 {
		return false
	}
	return true
}

// TODO : maybe it's better to decompose this func
// TryReadDatabaseFile tries to read file as an incremental data file if possible, otherwise just open the file
func TryReadDatabaseFile(fileName string, lsn *uint64, isNew bool) (fileReader io.ReadCloser, isPaged bool, size int64, err error) {
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

	pageReader := &IncrementalPageReader{make(chan []byte, 4), lim, file, info, *lsn, nil, nil}
	incrSize, err := pageReader.initialize()
	if err != nil {
		if err == InvalidBlockError {
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
	return pageReader, true, incrSize, nil
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
		return InvalidIncrementFileHeaderError
	}
	if header[2] != '1' {
		return UnknownIncrementFileHeaderError
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

	page := make([]byte, WalPageSize)
	for i := uint32(0); i < diffBlockCount; i++ {
		blockNo := binary.LittleEndian.Uint32(diffMap[i*sizeofInt32 : (i+1)*sizeofInt32])
		_, err = io.ReadFull(increment, page)
		if err != nil {
			return err
		}

		_, err = file.WriteAt(page, int64(blockNo)*int64(WalPageSize))
		if err != nil {
			return err
		}
	}

	all, _ := increment.Read(make([]byte, 1))
	if all > 0 {
		return UnexpectedTarDataError
	}

	return nil
}
