//
// This file provides low level routines for handling incremental backup
// Incremental file format is:
// 4 bytes header with designation information, format version and magic number
// 8 bytes uint file size
// 4 bytes uint changed pages count N
// (N * 4) bytes for Block Numbers of changed pages
// (N * DatabasePageSize) bytes for changed page data
//

package internal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"strings"

	"github.com/RoaringBitmap/roaring"
	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/limiters"
	"github.com/wal-g/wal-g/internal/walparser"
	"github.com/wal-g/wal-g/internal/walparser/parsingutil"
	"github.com/wal-g/wal-g/utility"
)

const (
	DatabasePageSize            = int64(walparser.BlockSize)
	sizeofInt32                 = 4
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
type InvalidBlockError struct {
	error
}

func newInvalidBlockError(blockNo uint32) InvalidBlockError {
	return InvalidBlockError{errors.Errorf("block %d is invalid", blockNo)}
}

func (err InvalidBlockError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type InvalidIncrementFileHeaderError struct {
	error
}

func newInvalidIncrementFileHeaderError() InvalidIncrementFileHeaderError {
	return InvalidIncrementFileHeaderError{errors.New("Invalid increment file header")}
}

func (err InvalidIncrementFileHeaderError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type UnknownIncrementFileHeaderError struct {
	error
}

func newUnknownIncrementFileHeaderError() UnknownIncrementFileHeaderError {
	return UnknownIncrementFileHeaderError{errors.New("Unknown increment file header")}
}

func (err UnknownIncrementFileHeaderError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type UnexpectedTarDataError struct {
	error
}

func newUnexpectedTarDataError() UnexpectedTarDataError {
	return UnexpectedTarDataError{errors.New("Expected end of Tar")}
}

func (err UnexpectedTarDataError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

var pagedFilenameRegexp *regexp.Regexp

func init() {
	pagedFilenameRegexp = regexp.MustCompile(`^(\d+)([.]\d+)?$`)
}

// TODO : unit tests
// isPagedFile checks basic expectations for paged file
func isPagedFile(info os.FileInfo, filePath string) bool {
	// For details on which file is paged see
	//nolint:lll    // https://www.postgresql.org/message-id/flat/F0627DEB-7D0D-429B-97A9-D321450365B4%40yandex-team.ru#F0627DEB-7D0D-429B-97A9-D321450365B4@yandex-team.ru
	if info.IsDir() ||
		((!strings.Contains(filePath, DefaultTablespace)) && (!strings.Contains(filePath, NonDefaultTablespace))) ||
		info.Size() == 0 ||
		info.Size()%DatabasePageSize != 0 ||
		!pagedFilenameRegexp.MatchString(path.Base(filePath)) {
		return false
	}
	return true
}

func ReadIncrementalFile(filePath string,
	fileSize int64,
	lsn uint64,
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

	pageReader := &IncrementalPageReader{fileReadSeekCloser, fileSize, lsn, nil, nil}
	incrementSize, err := pageReader.initialize(deltaBitmap)
	if err != nil {
		return nil, 0, err
	}
	return pageReader, incrementSize, nil
}

func ReadIncrementLocations(filePath string, fileSize int64, lsn uint64) ([]walparser.BlockLocation, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	fileReadSeekCloser := &ioextensions.ReadSeekCloserImpl{
		Reader: limiters.NewDiskLimitReader(file),
		Seeker: file,
		Closer: file,
	}
	pageReader := &IncrementalPageReader{fileReadSeekCloser, fileSize, lsn, nil, nil}
	err = pageReader.FullScanInitialize()
	if err != nil {
		return nil, err
	}
	return convertBlocksToLocations(filePath, pageReader.Blocks)
}

func convertBlocksToLocations(filePath string, blocks []uint32) ([]walparser.BlockLocation, error) {
	relFileNode, err := GetRelFileNodeFrom(filePath)
	if err != nil {
		return nil, err
	}
	locations := make([]walparser.BlockLocation, 0, len(blocks))
	for _, blockNo := range blocks {
		locations = append(locations, *walparser.NewBlockLocation(relFileNode.SpcNode,
			relFileNode.DBNode, relFileNode.RelNode, blockNo))
	}
	return locations, nil
}

// ApplyFileIncrement changes pages according to supplied change map file
func ApplyFileIncrement(fileName string, increment io.Reader, createNewIncrementalFiles bool) error {
	tracelog.DebugLogger.Printf("Incrementing %s\n", fileName)
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

	openFlags := os.O_RDWR
	if createNewIncrementalFiles {
		openFlags = openFlags | os.O_CREATE
	}

	file, err := os.OpenFile(fileName, openFlags, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.Wrap(err, "incremented file should always exist")
		}
		return errors.Wrap(err, "can't open file to increment")
	}
	defer utility.LoggedClose(file, "")
	defer utility.LoggedSync(file, "")

	err = file.Truncate(int64(fileSize))
	if err != nil {
		return err
	}

	page := make([]byte, DatabasePageSize)
	for i := uint32(0); i < diffBlockCount; i++ {
		blockNo := binary.LittleEndian.Uint32(diffMap[i*sizeofInt32 : (i+1)*sizeofInt32])
		_, err = io.ReadFull(increment, page)
		if err != nil {
			return err
		}

		_, err = file.WriteAt(page, int64(blockNo)*DatabasePageSize)
		if err != nil {
			return err
		}
	}

	all, _ := increment.Read(make([]byte, 1))
	if all > 0 {
		return newUnexpectedTarDataError()
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
		return newInvalidIncrementFileHeaderError()
	}
	if header[2] != '1' {
		return newUnknownIncrementFileHeaderError()
	}
	return nil
}
