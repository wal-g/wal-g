package greenplum

import (
	"bytes"
	"fmt"
	"io"
	"os"

	"github.com/wal-g/wal-g/internal/ioextensions"
	"github.com/wal-g/wal-g/internal/limiters"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/walparser/parsingutil"
	"github.com/wal-g/wal-g/utility"
)

const SignatureMagicNumber byte = 0x56
const sizeofInt32 = 4

type UnexpectedTarDataError struct {
	error
}

func newUnexpectedTarDataError() UnexpectedTarDataError {
	return UnexpectedTarDataError{errors.New("Expected end of Tar")}
}

func (err UnexpectedTarDataError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// IncrementFileHeader contains "wi" at the head which stands for "wal-g increment"
// format version "1", signature magic number
var IncrementFileHeader = []byte{'w', 'i', '1', SignatureMagicNumber}

type UnknownIncrementFileHeaderError struct {
	error
}

func newUnknownIncrementFileHeaderError() UnknownIncrementFileHeaderError {
	return UnknownIncrementFileHeaderError{errors.New("Unknown increment file header")}
}

func (err UnknownIncrementFileHeaderError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

type InvalidIncrementFileHeaderError struct {
	error
}

func newInvalidIncrementFileHeaderError(header []byte) InvalidIncrementFileHeaderError {
	return InvalidIncrementFileHeaderError{fmt.Errorf("invalid increment file header: %v", header)}
}

func (err InvalidIncrementFileHeaderError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

func ReadIncrementFileHeader(reader io.Reader) error {
	header := make([]byte, sizeofInt32)
	_, err := io.ReadFull(reader, header)
	if err != nil {
		return err
	}

	for i := range []int{0, 1, 3} {
		if header[i] != IncrementFileHeader[i] {
			return newInvalidIncrementFileHeaderError(header)
		}
	}

	if header[2] != IncrementFileHeader[2] {
		return newUnknownIncrementFileHeaderError()
	}
	return nil
}

func ApplyFileIncrement(fileName string, increment io.Reader, fsync bool) error {
	tracelog.DebugLogger.Printf("Incrementing AO/AOCS segment %s\n", fileName)
	err := ReadIncrementFileHeader(increment)
	if err != nil {
		return err
	}

	var eof uint64
	var offset uint64
	err = parsingutil.ParseMultipleFieldsFromReader([]parsingutil.FieldToParse{
		{Field: &eof, Name: "eof"},
		{Field: &offset, Name: "offset"},
	}, increment)
	if err != nil {
		return err
	}

	if eof <= offset {
		return fmt.Errorf("%s: parsed eof %d is smaller than offset %d", fileName, eof, offset)
	}

	openFlags := os.O_RDWR
	file, err := os.OpenFile(fileName, openFlags, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			return errors.Wrap(err, "incremented file should always exist")
		}
		return errors.Wrap(err, "can't open file to increment")
	}

	defer utility.LoggedClose(file, "")
	defer utility.LoggedSync(file, "", fsync)

	err = file.Truncate(int64(eof))
	if err != nil {
		return err
	}

	_, err = file.Seek(int64(offset), io.SeekStart)
	if err != nil {
		return err
	}

	_, err = io.CopyN(file, increment, int64(eof-offset))
	if err != nil {
		return err
	}

	all, _ := increment.Read(make([]byte, 1))
	if all > 0 {
		return newUnexpectedTarDataError()
	}

	return nil
}

func NewIncrementalPageReader(file io.ReadSeekCloser, eof, offset int64) (io.ReadCloser, error) {
	if eof <= offset {
		return nil, fmt.Errorf("file eof %d is less or equal than offset %d", eof, offset)
	}
	var headerBuffer bytes.Buffer
	headerBuffer.Write(IncrementFileHeader)
	headerBuffer.Write(utility.ToBytes(uint64(eof)))
	headerBuffer.Write(utility.ToBytes(uint64(offset)))

	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		utility.LoggedClose(file, "")
		return nil, err
	}

	return &ioextensions.ReadCascadeCloser{
		Reader: &io.LimitedReader{
			R: io.MultiReader(&headerBuffer, limiters.NewDiskLimitReader(file)),
			N: int64(headerBuffer.Len()) + eof - offset,
		},
		Closer: file,
	}, nil
}
