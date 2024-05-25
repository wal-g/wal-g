package innodb

import (
	"bytes"
	"github.com/wal-g/tracelog"
	"io"
	"os"
)

type RawPage struct {
	Header  FILHeader
	Trailer FILTrailer
	Payload io.ReadSeeker
}

type PageReader struct {
	file     *os.File
	SpaceID  SpaceID
	PageSize uint16
}

func NewPageReader(f *os.File) *PageReader {
	// read basic info:
	_, err := f.Seek(0, io.SeekStart)
	tracelog.ErrorLogger.FatalfOnError("seek: %v", err)

	// We need first 58 bytes of innodb file to get FSP_HDR...
	page := make([]byte, 64)
	_, err = io.ReadFull(f, page)
	tracelog.ErrorLogger.FatalfOnError("ReadFull %v", err)

	// reset file position:
	_, err = f.Seek(0, io.SeekStart)
	tracelog.ErrorLogger.FatalfOnError("seek: %v", err)

	// Parse first page:
	header := readHeader(page)
	if header.PageType != PageTypeFileSpaceHeader {
		tracelog.ErrorLogger.Fatalf("0-page in file %v is not FSP_HDR. Actual type: %v", nil, header.PageType)
	}
	fsp := readFileSpaceHeader(page)

	pageSize := fsp.Flags.pageSize()
	if pageSize == 0 {
		pageSize = InnoDBDefaultPageSize
	}

	return &PageReader{
		file:     f,
		SpaceID:  header.SpaceID,
		PageSize: pageSize,
	}
}

func (r *PageReader) ReadRaw(pn PageNumber) (RawPage, error) {
	var offset = int64(r.PageSize) * int64(pn)
	_, err := r.file.Seek(offset, io.SeekStart)
	tracelog.ErrorLogger.FatalfOnError("seek: %v", err)

	page := make([]byte, r.PageSize)
	_, err = r.file.Read(page)
	if err == io.EOF {
		return RawPage{}, err
	}
	// we don't expect UnexpectedEOF here (even compressed pages are always PageSize bytes)
	tracelog.ErrorLogger.FatalfOnError("read page: %v", err)

	return RawPage{
		Header:  readHeader(page),
		Trailer: readTrailer(page),
		Payload: bytes.NewReader(page),
	}, nil
}

func (r *PageReader) Close() error {
	return r.file.Close()
}
