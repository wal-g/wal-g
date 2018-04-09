package walg

import (
	"encoding/binary"
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
	"github.com/rasky/go-lzo"
	"io"
	"regexp"
)

// RaskyReader handles cases when the Rasky lzo package crashes.
// Occurs if byte size is too small (1-5).
type RaskyReader struct {
	R io.Reader
}

// Read ensures all bytes are get read for Rasky package.
func (r *RaskyReader) Read(p []byte) (int, error) {
	return io.ReadFull(r.R, p)
}

// Uncompressed is used to log compression ratio.
var Uncompressed uint32

// Compressed is used to log compression ratio.
var Compressed uint32

// CheckType grabs the file extension from PATH.
func CheckType(path string) string {
	re := regexp.MustCompile(`\.([^\.]+)$`)
	f := re.FindString(path)
	if f != "" {
		return f[1:]
	}
	return ""

}

// DecompressLzo decompresses an .lzo file. Returns the first error
// encountered.
func DecompressLzo(d io.Writer, s io.Reader) error {
	skip := 33
	sk := make([]byte, skip)

	n, err := io.ReadFull(s, sk)
	if n != len(sk) {
		return errors.New("DecompressLzo: did not fill skip")
	}
	if err != nil {
		return errors.Wrap(err, "DecompressLzo: read failed")
	}

	var fileNameLen uint8
	binary.Read(s, binary.BigEndian, &fileNameLen)
	fileName := make([]byte, fileNameLen)
	n, err = io.ReadFull(s, fileName)
	if n != len(fileName) {
		return errors.New("DecompressLzo: did not fill filename")
	}
	if err != nil {
		return errors.Wrap(err, "DecompressLzo: read failed")
	}

	fileComment := make([]byte, 4)
	n, err = io.ReadFull(s, fileComment)
	if n != len(fileComment) {
		return errors.New("DecompressLzo: did not fill fileComment")
	}
	if err != nil {
		return errors.Wrap(err, "DecompressLzo: read failed")
	}

	var uncom uint32
	var com uint32
	var check uint32

	for {

		err := binary.Read(s, binary.BigEndian, &uncom)
		if uncom == 0 {
			break
		}
		if err != nil {
			return errors.Wrap(err, "DecompressLzo: read failed")
		}

		err = binary.Read(s, binary.BigEndian, &com)
		if err != nil {
			return errors.Wrap(err, "DecompressLzo: read failed")
		}

		Uncompressed += uncom
		Compressed += com

		err = binary.Read(s, binary.BigEndian, &check)
		if err != nil {
			return errors.Wrap(err, "DecompressLzo: read failed")
		}

		if uncom <= com {
			n, err := io.CopyN(d, s, int64(com))
			if n != int64(com) {
				return errors.New("DecompressLzo: copy failed")
			}
			if err != nil {
				return errors.Wrap(err, "DecompressLzo: copy failed")
			}

		} else {
			ras := &RaskyReader{
				R: s,
			}

			out, err := lzo.Decompress1X(ras, int(com), int(uncom))
			if err != nil {
				return errors.Wrap(err, "DecompressLzo: decompress lzo failed")
			}

			if len(out) != int(uncom) {
				return errors.New("DecompressLzo: out bytes do not equal uncompressed")
			}

			n, err = d.Write(out)
			if n != len(out) {
				return errors.New("DecompressLzo: write to pipe failed")
			}
			if err != nil {
				return errors.Wrap(err, "DecompressLzo: write to pipe failed")
			}
		}
	}
	return nil
}

// DecompressLz4 decompresses a .lz4 file. Returns an error upon failure.
func DecompressLz4(d io.Writer, s io.Reader) (int64, error) {
	lz := lz4.NewReader(s)
	n, err := lz.WriteTo(d)
	if err != nil {
		return n, errors.Wrap(err, "DecompressLz4: lz4 write failed")
	}
	return n, nil
}

// ReadCascadeClose composes io.ReadCloser from two parts
type ReadCascadeClose struct {
	io.Reader
	io.Closer
}
