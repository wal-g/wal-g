package walg

import (
	"fmt"
	"github.com/pkg/errors"
	"io"
)

// CompressingPipeWriterError is used to catch specific errors from CompressingPipeWriter
// when uploading to S3. Will not retry upload if this error
// occurs.
type CompressingPipeWriterError struct {
	err error
}

func (err CompressingPipeWriterError) Error() string {
	msg := fmt.Sprintf("%+v\n", err.err)
	return msg
}

// CompressingPipeWriter allows for flexibility of using compressed output.
// Input is read and compressed to a pipe reader.
type CompressingPipeWriter struct {
	Input                io.Reader
	Output               io.Reader
	NewCompressingWriter func(io.Writer) ReaderFromWriteCloser
}

// Compress compresses input to a pipe reader. Output must be used or
// pipe will block.
func (pipeWriter *CompressingPipeWriter) Compress(crypter Crypter) {
	var dstWriter *io.PipeWriter
	pipeWriter.Output, dstWriter = io.Pipe()

	var writeCloser io.WriteCloser = dstWriter
	if crypter.IsUsed() {
		var err error
		writeCloser, err = crypter.Encrypt(dstWriter)

		if err != nil {
			panic(err)
		}
	}

	writeIgnorer := &EmptyWriteIgnorer{writeCloser}
	lzWriter := pipeWriter.NewCompressingWriter(writeIgnorer)

	go func() {
		_, err := lzWriter.ReadFrom(pipeWriter.Input)

		if err != nil {
			e := CompressingPipeWriterError{errors.Wrap(err, "Compress: compression failed")}
			dstWriter.CloseWithError(e)
		}

		if err := lzWriter.Close(); err != nil {
			e := CompressingPipeWriterError{errors.Wrap(err, "Compress: writer close failed")}
			dstWriter.CloseWithError(e)
			return
		}
		if crypter.IsUsed() {
			err := writeCloser.Close()

			if err != nil {
				e := CompressingPipeWriterError{errors.Wrap(err, "Compress: encryption failed")}
				dstWriter.CloseWithError(e)
				return
			}
		}
		if err = dstWriter.Close(); err != nil {
			e := CompressingPipeWriterError{errors.Wrap(err, "Compress: pipe writer close failed")}
			dstWriter.CloseWithError(e)
		}
	}()
}
