package libsodium

// #cgo CFLAGS: -I../../../tmp/libsodium/include
// #cgo LDFLAGS: -L../../../tmp/libsodium/lib -lsodium
// #include <sodium.h>
import "C"

import (
	"io"

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
}

// NewReader creates Reader from ordinary reader and key
func NewReader(reader io.Reader, key []byte) (io.Reader, error) {
	header := make([]byte, C.crypto_secretstream_xchacha20poly1305_HEADERBYTES)

	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, errors.Wrap(err, "failed to read libsodium header")
	}

	var state C.crypto_secretstream_xchacha20poly1305_state

	returnCode := C.crypto_secretstream_xchacha20poly1305_init_pull(
		&state,
		(*C.uchar)(&header[0]),
		(*C.uchar)(&key[0]),
	)

	if returnCode != 0 {
		return nil, errors.New("corrupted libsodium header")
	}

	return &Reader{
		Reader: reader,

		state: state,

		in:  make([]byte, chunkSize+C.crypto_secretstream_xchacha20poly1305_ABYTES),
		out: make([]byte, chunkSize),
	}, nil
}

// Read implements io.Reader
func (reader *Reader) Read(p []byte) (n int, err error) {
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

	reader.in = reader.in[:n]

	if err != nil && err != io.ErrUnexpectedEOF {
		return
	}

	var outLen C.ulonglong
	var tag C.uchar

	returnCode := C.crypto_secretstream_xchacha20poly1305_pull(
		&reader.state,
		(*C.uchar)(&reader.out[0]),
		(*C.ulonglong)(&outLen),
		(*C.uchar)(&tag),
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

	return
}
