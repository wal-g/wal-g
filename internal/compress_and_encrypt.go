package internal

import (
	"fmt"
	"io"

	"github.com/pkg/errors"
	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/utility"
)

// CompressAndEncryptError is used to catch specific errors from CompressAndEncrypt
// when uploading to Storage. Will not retry upload if this error occurs.
type CompressAndEncryptError struct {
	error
}

func newCompressingPipeWriterError(reason string, cause error) CompressAndEncryptError {
	err := errors.Wrap(cause, reason)
	if err == nil {
		err = errors.New(reason)
	}
	return CompressAndEncryptError{err}
}

func (err CompressAndEncryptError) Error() string {
	return fmt.Sprintf(tracelog.GetErrorFormatter(), err.error)
}

// CompressAndEncrypt compresses input to a pipe reader. Output must be used or
// pipe will block.
func CompressAndEncrypt(source io.Reader, compressor compression.Compressor, crypter crypto.Crypter) io.Reader {
	compressedReader, dstWriter := io.Pipe()

	var writeCloser io.WriteCloser = dstWriter
	if crypter != nil {
		var err error
		writeCloser, err = crypter.Encrypt(dstWriter)

		if err != nil {
			panic(err)
		}
	}

	var compressedWriter io.WriteCloser
	if compressor != nil {
		writeIgnorer := &utility.EmptyWriteIgnorer{Writer: writeCloser}
		compressedWriter = compressor.NewWriter(writeIgnorer)
	} else {
		compressedWriter = writeCloser
	}

	go func() {
		if _, err := utility.FastCopy(compressedWriter, source); err != nil {
			e := newCompressingPipeWriterError("CompressAndEncrypt: compression failed", err)
			_ = dstWriter.CloseWithError(e)
		}

		if err := compressedWriter.Close(); err != nil {
			e := newCompressingPipeWriterError("CompressAndEncrypt: writer close failed", err)
			_ = dstWriter.CloseWithError(e)
			return
		}
		if crypter != nil {
			if err := writeCloser.Close(); err != nil {
				e := newCompressingPipeWriterError("CompressAndEncrypt: encryption failed", err)
				_ = dstWriter.CloseWithError(e)
				return
			}
		}
		_ = dstWriter.Close()
	}()
	return compressedReader
}
