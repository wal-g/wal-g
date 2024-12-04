package innodb

import (
	"bytes"
	"encoding/hex"
	"fmt"
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

func NewPageReader(f *os.File) (*PageReader, error) {
	// read basic info:
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek on file %v: %w", f.Name(), err)
	}

	// We need first 58 bytes of innodb file to get FSP_HDR...
	page := make([]byte, 64)
	_, err = io.ReadFull(f, page)
	if err != nil {
		return nil, fmt.Errorf("cannot read first innodb page on file %v: %w", f.Name(), err)
	}

	// reset file position:
	_, err = f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("seek on file %v: %w", f.Name(), err)
	}

	// Parse first page:
	header := readHeader(page)
	if header.PageType != PageTypeFileSpaceHeader {
		return nil, fmt.Errorf("0-page in file %v is not FSP_HDR. Actual type: %v\n%v",
			f.Name(), header.PageType, hex.EncodeToString(page))
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
	}, nil
}

func (r *PageReader) ReadRaw(pn PageNumber) (RawPage, error) {
	var offset = int64(r.PageSize) * int64(pn)
	_, err := r.file.Seek(offset, io.SeekStart)
	if err != nil {
		return RawPage{}, fmt.Errorf("seek on file %v: %w", r.file.Name(), err)
	}

	page := make([]byte, r.PageSize)
	_, err = r.file.Read(page)
	if err == io.EOF {
		return RawPage{}, err
	}
	// we don't expect UnexpectedEOF here (even compressed pages are always PageSize bytes)
	if err != nil {
		return RawPage{}, fmt.Errorf("read page %v: %w", r.file.Name(), err)
	}

	return RawPage{
		Header:  readHeader(page),
		Trailer: readTrailer(page),
		Payload: bytes.NewReader(page),
	}, nil
}

func (r *PageReader) Close() error {
	return r.file.Close()
}
