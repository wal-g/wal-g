package libsodium

// #cgo CFLAGS: -I../../../tmp/libsodium/include
// #cgo LDFLAGS: -L../../../tmp/libsodium/lib -lsodium
// #include <sodium.h>
import "C"

import (
	"io"
	"sync"

	"github.com/pkg/errors"
)

// Writer wraps ordinary writer with libsodium encryption
type Writer struct {
	io.Writer

	state C.crypto_secretstream_xchacha20poly1305_state

	in  []byte
	out []byte

	inIdx int

	// In case of using io.Pipe we can't write header until reader doesn't read, therefor we use these sync
	o         sync.Once
	key       []byte
	headerErr error
}

// NewWriter creates Writer from ordinary writer and key
func NewWriter(writer io.Writer, key []byte) io.WriteCloser {
	return &Writer{
		Writer: writer,

		in:  make([]byte, chunkSize),
		out: make([]byte, chunkSize+C.crypto_secretstream_xchacha20poly1305_ABYTES),
		key: key,
	}
}

func (writer *Writer) writeHeader() {
	header := make([]byte, C.crypto_secretstream_xchacha20poly1305_HEADERBYTES)

	var state C.crypto_secretstream_xchacha20poly1305_state

	C.crypto_secretstream_xchacha20poly1305_init_push(
		&state,
		(*C.uchar)(&header[0]),
		(*C.uchar)(&writer.key[0]),
	)

	if _, err := writer.Writer.Write(header); err != nil {
		writer.headerErr = errors.Wrap(err, "failed to write libsodium header")
		return
	}
	writer.state = state
}

// Write implements io.Writer
func (writer *Writer) Write(p []byte) (n int, err error) {
	writer.o.Do(writer.writeHeader)
	if writer.headerErr != nil {
		return 0, err
	}

	for n != len(p) {
		count := copy(writer.in[writer.inIdx:], p[n:])

		writer.inIdx += count
		n += count

		if writer.inIdx == len(writer.in) {
			if err = writer.writeNextChunk(false); err != nil {
				return
			}
		}
	}

	return
}

func (writer *Writer) writeNextChunk(last bool) (err error) {
	var outLen C.ulonglong
	var tag C.uchar

	if last {
		tag = C.crypto_secretstream_xchacha20poly1305_TAG_FINAL
	}

	C.crypto_secretstream_xchacha20poly1305_push(
		&writer.state,
		(*C.uchar)(&writer.out[0]),
		(*C.ulonglong)(&outLen),
		(*C.uchar)(&writer.in[0]),
		(C.ulonglong)(writer.inIdx),
		(*C.uchar)(C.NULL),
		(C.ulonglong)(0),
		(C.uchar)(tag),
	)

	if _, err = writer.Writer.Write(writer.out[:int(outLen)]); err != nil {
		return
	}

	writer.inIdx = 0

	return
}

// Close implements io.Closer
func (writer *Writer) Close() (err error) {
	if closer, ok := writer.Writer.(io.WriteCloser); ok {
		defer closer.Close()
	}
	return writer.writeNextChunk(true)
}
