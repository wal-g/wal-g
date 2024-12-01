package innodb

import (
	"errors"
	"io"
	"os"
	"strings"
	"syscall"

	"github.com/wal-g/tracelog"

	"github.com/wal-g/wal-g/internal/ioextensions"
)

func RepairSparse(file *os.File) error {
	if !strings.HasSuffix(file.Name(), "ibd") {
		return nil
	}
	_, err := file.Seek(0, io.SeekStart)
	if err != nil {
		return err
	}

	pageReader, err := NewPageReader(file)
	tracelog.ErrorLogger.FatalOnError(err)
	pageNumber := 1 // Never compress/decompress the first page (FSP_HDR)
	for {
		page, err := pageReader.ReadRaw(PageNumber(pageNumber))
		if err == io.EOF {
			return nil
		}
		pageNumber++
		tracelog.ErrorLogger.FatalOnError(err) // FIXME: in future we can ignore such errors

		if page.Header.PageType == PageTypeCompressed {
			// do punch hole, if possible
			meta := page.Header.GetCompressedData()
			if meta.CompressedSize < pageReader.PageSize {
				offset := int64(page.Header.PageNumber)*int64(pageReader.PageSize) + int64(meta.CompressedSize)
				size := int64(pageReader.PageSize - meta.CompressedSize)
				err = ioextensions.PunchHole(file, offset, size)
				if errors.Is(err, syscall.EOPNOTSUPP) {
					return nil // ok
				}
				tracelog.ErrorLogger.FatalfOnError("fallocate: %v", err)
			}
		}
	}
}
