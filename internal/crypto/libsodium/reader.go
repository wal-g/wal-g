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

// Reader wraps ordinary reader with libsodium decryption
type Reader struct {
	io.Reader

	state C.crypto_secretstream_xchacha20poly1305_state

	in  []byte
	out []byte

	outIdx int
	outLen int

	// In case of using io.Pipe we can't read header until writer doesn't write, therefor we use these sync
	onceHeader sync.Once
	key        []byte
	headerErr  error
}

// NewReader creates Reader from ordinary reader and key
func NewReader(reader io.Reader, key []byte) io.Reader {
	return &Reader{
		Reader: reader,

		in:  make([]byte, chunkSize+C.crypto_secretstream_xchacha20poly1305_ABYTES),
		out: make([]byte, chunkSize),

		key: key,
	}
}

func (reader *Reader) readHeader() {
	header := make([]byte, C.crypto_secretstream_xchacha20poly1305_HEADERBYTES)

	if _, err := io.ReadFull(reader.Reader, header); err != nil {
		reader.headerErr = errors.Wrap(err, "failed to read libsodium header")
		return
	}

	var state C.crypto_secretstream_xchacha20poly1305_state

	returnCode := C.crypto_secretstream_xchacha20poly1305_init_pull(
		&state,
		(*C.uchar)(&header[0]),
		(*C.uchar)(&reader.key[0]),
	)

	if returnCode != 0 {
		reader.headerErr = errors.New("corrupted libsodium header")
		return
	}

	reader.state = state
}

// Read implements io.Reader
func (reader *Reader) Read(p []byte) (n int, err error) {
	reader.onceHeader.Do(reader.readHeader)
	if reader.headerErr != nil {
		return 0, reader.headerErr
	}

	if reader.outIdx >= reader.outLen {
		if err = reader.readNextChunk(); err != nil {
			return
		}
	}

	n = copy(p, reader.out[reader.outIdx:reader.outLen])
	reader.outIdx += n

	return
}

func (reader *Reader) readNextChunk() (err error) {
	n, err := io.ReadFull(reader.Reader, reader.in)

	if err != nil && err != io.ErrUnexpectedEOF {
		return
	}

	var outLen C.ulonglong
	var tag C.uchar

	returnCode := C.crypto_secretstream_xchacha20poly1305_pull(
		&reader.state,
		(*C.uchar)(&reader.out[0]),
		&outLen,
		&tag,
		(*C.uchar)(&reader.in[0]),
		(C.ulonglong)(n),
		(*C.uchar)(C.NULL),
		(C.ulonglong)(0),
	)

	if returnCode != 0 {
		err = errors.New("corrupted chunk")
	}

	if tag == C.crypto_secretstream_xchacha20poly1305_TAG_FINAL && err != io.ErrUnexpectedEOF {
		err = errors.New("premature end")
	}

	if err == io.ErrUnexpectedEOF {
		err = nil
	}

	reader.outIdx = 0
	reader.outLen = int(outLen)

	return err
}
