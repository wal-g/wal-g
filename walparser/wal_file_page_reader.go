package walparser

import (
	"io"
	"github.com/pkg/errors"
)

type WalPageReader struct {
	walFileReader io.Reader
}

func NewWalPageReader(walFileReader io.Reader) *WalPageReader {
	return &WalPageReader{walFileReader}
}

// reads data corresponding to one page
func (reader *WalPageReader) ReadPageData() ([]byte, error) {
	page := make([]byte, WalPageSize)
	_, err := io.ReadFull(reader.walFileReader, page)
	if err != nil && err != io.EOF {
		return nil, errors.WithStack(err)
	}
	return page, err
}
