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
	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"github.com/wal-g/wal-g/walparser"
	"github.com/wal-g/wal-g/walparser/parsingutil"
	"io"
	"os"
	"path"
	"regexp"
	"strings"
)

const (
	WalPageSize                 = walparser.WalPageSize
	sizeofInt32                 = 4
	sizeofInt16                 = 2
	sizeofInt64                 = 8
	SignatureMagicNumber byte   = 0x55
	invalidLsn           uint64 = 0
	validFlags                  = 7
	layoutVersion               = 4
	headerSize                  = 24

	DefaultTablespace    = "base"
	GlobalTablespace     = "global"
	NonDefaultTablespace = "pg_tblspc"
)

// InvalidBlockError indicates that file contain invalid page and cannot be archived incrementally
var InvalidBlockError = errors.New("block is invalid")
var InvalidIncrementFileHeaderError = errors.New("Invalid increment file header")
var UnknownIncrementFileHeaderError = errors.New("Unknown increment file header")
var UnexpectedTarDataError = errors.New("Expected end of Tar")

var pagedFilenameRegexp *regexp.Regexp

func init() {
	pagedFilenameRegexp = regexp.MustCompile("^(\\d+)([.]\\d+)?$")
}

// TODO : unit tests
// isPagedFile checks basic expectations for paged file
func isPagedFile(info os.FileInfo, filePath string) bool {

	// For details on which file is paged see
	// https://www.postgresql.org/message-id/flat/F0627DEB-7D0D-429B-97A9-D321450365B4%40yandex-team.ru#F0627DEB-7D0D-429B-97A9-D321450365B4@yandex-team.ru
	if info.IsDir() ||
		((!strings.Contains(filePath, DefaultTablespace)) && (!strings.Contains(filePath, GlobalTablespace)) && (!strings.Contains(filePath, NonDefaultTablespace))) ||
		info.Size() == 0 ||
		info.Size()%int64(WalPageSize) != 0 ||
		!pagedFilenameRegexp.MatchString(path.Base(filePath)) {
		return false
	}
	return true
}

// TODO : unit tests
// ReadDatabaseFile reads file as an incremental data file
func ReadDatabaseFile(filePath string, fileSize int64, lsn uint64, deltaBitmap *roaring.Bitmap) (fileReader io.ReadCloser, size int64, err error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, 0, err
	}

	limitedFileReader := &io.LimitedReader{
		R: io.MultiReader(NewDiskLimitReader(file), &ZeroReader{}),
		N: int64(fileSize),
	}

	pageReader := &IncrementalPageReader{make(chan []byte, 4), limitedFileReader, file, fileSize, lsn, nil, nil}
	incrementSize, err := pageReader.initialize(deltaBitmap)
	if err != nil {
		return nil, 0, err
	}
	return pageReader, incrementSize, nil
}

// TODO : unit tests
// ApplyFileIncrement changes pages according to supplied change map file
func ApplyFileIncrement(fileName string, increment io.Reader) error {
	fmt.Printf("Incrementing %s\n", fileName)
	err := ReadIncrementFileHeader(increment)
	if err != nil {
		return err
	}

	var fileSize uint64
	var diffBlockCount uint32
	err = parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &fileSize, Name: "fileSize"},
		{Field: &diffBlockCount, Name: "diffBlockCount"},
	}, increment)
	if err != nil {
		return err
	}

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

func ReadIncrementFileHeader(reader io.Reader) error {
	header := make([]byte, sizeofInt32)
	_, err := io.ReadFull(reader, header)
	if err != nil {
		return err
	}

	if header[0] != 'w' || header[1] != 'i' || header[3] != SignatureMagicNumber {
		return InvalidIncrementFileHeaderError
	}
	if header[2] != '1' {
		return UnknownIncrementFileHeaderError
	}
	return nil
}
