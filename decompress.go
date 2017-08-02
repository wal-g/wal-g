package walg

import (
	"encoding/binary"
	"github.com/pierrec/lz4"
	"github.com/pkg/errors"
	"github.com/rasky/go-lzo"
	"io"
	"regexp"
)

/**
 *  Fix bug in rasky package. Rasky crashes if byte size is too small.
 */
type RaskyReader struct {
	R io.Reader
}

func (r *RaskyReader) Read(p []byte) (int, error) {
	return io.ReadFull(r.R, p)
}

var Uncompressed uint32
var Compressed uint32

/**
 *  Grabs the file extension from PATH
 */
func CheckType(path string) string {
	re := regexp.MustCompile(`\.([^\.]+)$`)
	f := re.FindString(path)
	return f[1:]
}

/**
 *  Decompress an .lzo file.
 */
func DecompressLzo(d io.Writer, s io.Reader) error {
	var err error
	skip := 33
	sk := make([]byte, skip)

	n, e := s.Read(sk)
	if n != len(sk) {
		err = errors.New("DecompressLzo: did not fill skip")
		return err
	}
	if e != nil {
		err = errors.Wrap(e, "DecompressLzo: read failed")
		return err
	}

	var fileNameLen uint8
	binary.Read(s, binary.BigEndian, &fileNameLen)
	fileName := make([]byte, fileNameLen)
	n, e = s.Read(fileName)
	if n != len(fileName) {
		err = errors.New("DecompressLzo: did not fill filename")
		return err
	}
	if e != nil {
		err = errors.Wrap(e, "DecompressLzo: read failed")
		return err
	}

	fileComment := make([]byte, 4)
	n, e = s.Read(fileComment)
	if n != len(fileComment) {
		err = errors.New("DecompressLzo: did not fill fileComment")
		return err
	}
	if e != nil {
		err = errors.Wrap(e, "DecompressLzo: read failed")
		return err
	}

	var uncom uint32
	var com uint32
	var check uint32

	for {

		e := binary.Read(s, binary.BigEndian, &uncom)
		if uncom == 0 {
			break
		}
		if e != nil {
			err = errors.Wrap(e, "DecompressLzo: read failed")
			return err
		}

		e = binary.Read(s, binary.BigEndian, &com)
		if err != nil {
			err = errors.Wrap(e, "DecompressLzo: read failed")
			return err
		}

		Uncompressed += uncom
		Compressed += com

		e = binary.Read(s, binary.BigEndian, &check)
		if e != nil {
			err = errors.Wrap(e, "DecompressLzo: read failed")
			return err
		}

		if uncom <= com {
			n, e := io.CopyN(d, s, int64(com))
			if n != int64(com) {
				err = errors.New("DecompressLzo: copy failed")
				return err
			}
			if e != nil {
				err = errors.Wrap(e, "DecompressLzo: copy failed")
				return err
			}

		} else {
			ras := &RaskyReader{
				R: s,
			}

			out, e := lzo.Decompress1X(ras, int(com), int(uncom))
			if e != nil {
				err = errors.Wrap(e, "DecompressLzo: decompress lzo failed")
				return err
			}
			if len(out) != int(uncom) {
				err = errors.New("DecompressLzo: out bytes do not equal uncompressed")
				return err
			}

			n, e = d.Write(out)
			if n != len(out) {
				err = errors.New("DecompressLzo: write to pipe failed")
				return err
			}
			if e != nil {
				err = errors.Wrap(e, "DecompressLzo: write to pipe failed")
				return err
			}
		}
	}
	return nil
}

/**
 *  Decompress a .lz4 file.
 */
func DecompressLz4(d io.Writer, s io.Reader) error {
	var err error
	lz := lz4.NewReader(s)

	_, e := lz.WriteTo(d)
	if e != nil {
		err = errors.Wrap(e, "DecompressLz4: lz4 write failed")
		return err
	}
	return err
}
