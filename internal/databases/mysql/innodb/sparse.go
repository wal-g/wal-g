package innodb

import (
	"errors"
	"fmt"
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
	blockSize, err := ioextensions.FileBlockSize(file)
	if errors.Is(err, syscall.EOPNOTSUPP) {
		return nil
	}
	if err != nil {
		return err
	}
	return repairSparse(file, blockSize, ioextensions.PunchHole)
}

func repairSparse(file *os.File, blockSize int64, punchHole func(*os.File, int64, int64) error) error {
	if blockSize <= 0 {
		return fmt.Errorf("invalid filesystem block size %d", blockSize)
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
			// FIL_PAGE_COMPRESS_SIZE_V1 excludes the 38-byte FIL header.
			// Preserve it and the final partial filesystem block, just as
			// XtraBackup's restore_sparseness does. Punching earlier corrupts
			// the compressed payload (fallocate also zeroes partial blocks).
			compressedEnd := int64(FILHeaderSize) + int64(meta.CompressedSize)
			compressedEnd = (compressedEnd + blockSize - 1) / blockSize * blockSize
			if compressedEnd < int64(pageReader.PageSize) {
				offset := int64(page.Header.PageNumber)*int64(pageReader.PageSize) + compressedEnd
				size := int64(pageReader.PageSize) - compressedEnd
				err = punchHole(file, offset, size)
				if errors.Is(err, syscall.EOPNOTSUPP) {
					return nil // ok
				}
				tracelog.ErrorLogger.FatalfOnError("fallocate: %v", err)
			}
		}
	}
}
