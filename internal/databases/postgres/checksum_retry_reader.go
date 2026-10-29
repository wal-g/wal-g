package postgres

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/limiters"
)

// checksumRetryReader verifies a page before releasing its bytes to the archive.
// Increment headers are preserved; rereads use relation-file offsets, not offsets
// into the encoded increment. Only one page is buffered at a time.
type checksumRetryReader struct {
	ctx           context.Context //nolint:containedctx // io.Reader has no context parameter; lifetime is one file upload
	path          string
	info          os.FileInfo
	source        io.ReadCloser
	retryFile     *os.File
	maxRetries    int
	prefix        bytes.Buffer
	diffMap       []byte
	blockCount    uint32
	nextBlock     uint32
	page          PgDatabasePage
	pending       []byte
	corruptBlocks []uint32
	reread        func(uint32, []byte) error
}

func newChecksumRetryReader(ctx context.Context, path string, info os.FileInfo,
	source io.ReadCloser, incremented bool, maxRetries int) (*checksumRetryReader, error) {
	r := &checksumRetryReader{ctx: ctx, path: path, info: info, source: source, maxRetries: maxRetries}
	r.reread = r.rereadPage
	if incremented {
		_, count, diffMap, err := GetIncrementHeaderFields(io.TeeReader(source, &r.prefix))
		if err != nil {
			return nil, err
		}
		r.blockCount, r.diffMap = count, diffMap
	} else {
		r.blockCount = uint32(info.Size() / DatabasePageSize)
	}
	return r, nil
}

func (r *checksumRetryReader) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	if r.prefix.Len() > 0 {
		return r.prefix.Read(p)
	}
	if len(r.pending) == 0 {
		if r.nextBlock == r.blockCount {
			return r.source.Read(p)
		}
		blockNo := r.nextBlock
		if r.diffMap != nil {
			blockNo = binary.LittleEndian.Uint32(r.diffMap[r.nextBlock*sizeofInt32:])
		}
		if err := r.readPage(blockNo); err != nil {
			return 0, err
		}
		r.nextBlock++
		r.pending = r.page[:DatabasePageSize]
	}
	n := copy(p, r.pending)
	r.pending = r.pending[n:]
	return n, nil
}

func (r *checksumRetryReader) readPage(blockNo uint32) error {
	if _, err := io.ReadFull(r.source, r.page[:DatabasePageSize]); err != nil {
		return err
	}
	for attempt := 0; ; attempt++ {
		if err := r.ctx.Err(); err != nil {
			return err
		}
		corrupt, err := r.checkPage(blockNo, attempt)
		if err != nil {
			return err
		}
		if !corrupt {
			return nil
		}
		if attempt == r.maxRetries {
			r.corruptBlocks = append(r.corruptBlocks, blockNo)
			return nil
		}
		if err := r.reread(blockNo, r.page[:DatabasePageSize]); err != nil {
			return fmt.Errorf("reread %s block %d: %w", r.path, blockNo, err)
		}
	}
}

// Once a mismatch has been observed, an invalid or checksum-less header on a
// reread is not evidence of recovery. A fully zero page can result from truncation.
func (r *checksumRetryReader) checkPage(blockNo uint32, attempt int) (bool, error) {
	corrupt, err := checkPageCorruption(r.path, blockNo, &r.page, attempt == r.maxRetries)
	if err != nil || corrupt || attempt == 0 || r.page == (PgDatabasePage{}) {
		return corrupt, err
	}
	header, err := parsePostgresPageHeader(bytes.NewReader(r.page[:]))
	if err != nil {
		return false, err
	}
	unverifiable := header.isNew() || !header.isValid() || header.pdChecksum == 0
	if unverifiable && attempt == r.maxRetries {
		tracelog.WarningLogger.Printf("Page remains unverifiable after %d checksum retries: %s/[%d]", r.maxRetries, r.path, blockNo)
	}
	return unverifiable, nil
}

func (r *checksumRetryReader) rereadPage(blockNo uint32, page []byte) error {
	if r.retryFile == nil {
		file, err := os.Open(r.path)
		if err != nil {
			return err
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return err
		}
		// A pathname might have been removed and reused while the backup was reading.
		// Never replace a page with data from a different relation-file inode.
		if !os.SameFile(r.info, info) {
			_ = file.Close()
			return fmt.Errorf("file was replaced during backup")
		}
		r.retryFile = file
	}
	reader := io.NewSectionReader(r.retryFile, int64(blockNo)*DatabasePageSize, DatabasePageSize)
	n, err := io.ReadFull(limiters.NewDiskLimitReader(r.ctx, reader), page)
	if err == io.EOF && n == 0 {
		// As in StartReadingFile, preserve the tar size if the relation was truncated.
		clear(page)
		return nil
	}
	return err
}

func (r *checksumRetryReader) Close() error {
	err := r.source.Close()
	if r.retryFile != nil {
		err = errors.Join(err, r.retryFile.Close())
	}
	return err
}
