package wal_parser

import (
	"io"
)

type WalPageReader struct {
	walFileReader io.Reader
}

// reads data corresponding to one page
func (reader *WalPageReader) ReadPageData() ([]byte, error) {
	page := make([]byte, WalPageSize)
	_, err := io.ReadFull(reader.walFileReader, page)
	if err != nil {
		return nil, err
	}
	return page, nil
}

