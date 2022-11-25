package internal

import (
	"fmt"
	"io"

	"github.com/wal-g/tracelog"
	"github.com/wal-g/wal-g/internal/compression"
	"github.com/wal-g/wal-g/internal/crypto"
	"github.com/wal-g/wal-g/utility"
)

// CompressAndEncryptError is used to catch specific errors from CompressAndEncrypt
// when uploading to Storage. Will not retry upload if this error occurs.
type CompressAndEncryptError struct {
	action string
	inner  error
}

func newCompressingPipeWriterError(action string, err error) CompressAndEncryptError {
	return CompressAndEncryptError{action, err}
}

func (e CompressAndEncryptError) Error() string {
	return fmt.Sprintf("CompressAndEncrypt: %s: "+tracelog.GetErrorFormatter(), e.action, e.inner)
}

func (e CompressAndEncryptError) Unwrap() error {
	return e.inner
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
		_, err := utility.FastCopy(compressedWriter, source)

		if err != nil {
			e := newCompressingPipeWriterError("compress", err)
			_ = dstWriter.CloseWithError(e)
		}

		if err := compressedWriter.Close(); err != nil {
			e := newCompressingPipeWriterError("close", err)
			_ = dstWriter.CloseWithError(e)
			return
		}
		if crypter != nil {
			err := writeCloser.Close()

			if err != nil {
				e := newCompressingPipeWriterError("encrypt", err)
				_ = dstWriter.CloseWithError(e)
				return
			}
		}
		_ = dstWriter.Close()
	}()
	return compressedReader
}
