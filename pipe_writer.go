package walg

import (
	"io"
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
	"fmt"
)

// CompressingPipeWriter allows for flexibility of using compressed output.
// Input is read and compressed to a pipe reader.
type CompressingPipeWriter struct {
	Input  io.Reader
	Output io.Reader
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
	lzWriter := lz4.NewWriter(writeIgnorer)

	go func() {
		_, err := lzWriter.ReadFrom(pipeWriter.Input)

		if err != nil {
			e := CompressingPipeWriterError{errors.Wrap(err, "Compress: compression failed")}
			dstWriter.CloseWithError(e)
		}

		defer func() {
			if err == nil {
				if err := lzWriter.Close(); err != nil {
					e := CompressingPipeWriterError{errors.Wrap(err, "Compress: writer close failed")}
					dstWriter.CloseWithError(e)
				} else {
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
				}
			}
		}()

	}()
}

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