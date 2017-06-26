package extract

import (
	"encoding/binary"
	"github.com/rasky/go-lzo"
	"io"
)

type RaskyReader struct {
	R io.Reader
}

func (r *RaskyReader) Read(p []byte) (int, error) {
	return io.ReadFull(r.R, p)
}

var Uncompressed uint32
var Compressed uint32

func decompress(w io.Writer, s io.Reader) {
	var skip int = 33
	sk := make([]byte, skip)
	n, err := s.Read(sk)
	if n != len(sk) {
		panic("Did not fill skip")
	}
	if err != nil {
		panic(err)
	}

	var fileNameLen uint8
	binary.Read(s, binary.BigEndian, &fileNameLen)
	fileName := make([]byte, fileNameLen)
	n, err = s.Read(fileName)
	if n != len(fileName) {
		panic("Did not fill filename")
	}
	if err != nil {
		panic(err)
	}

	fileComment := make([]byte, 4)
	n, err = s.Read(fileComment)
	if n != len(fileComment) {
		panic("Did not fill fileComment")
	}
	if err != nil {
		panic(err)
	}

	var uncom uint32
	var com uint32
	var check uint32

	for {

		err = binary.Read(s, binary.BigEndian, &uncom)
		if uncom == 0 {
			break
		}
		if err != nil {
			panic(err)
		}

		err = binary.Read(s, binary.BigEndian, &com)
		if err != nil {
			panic(err)
		}

		Uncompressed += uncom
		Compressed += com

		err = binary.Read(s, binary.BigEndian, &check)
		if err != nil {
			panic(err)
		}

		if uncom <= com {
			n, err := io.CopyN(w, s, int64(com))
			if n != int64(com) {
				panic("uncom <= com")
			}
			if err != nil {
				panic(err)
			}

		} else {
			ras := &RaskyReader{
				R: s,
			}

			out, err := lzo.Decompress1X(ras, int(com), int(uncom))
			if len(out) != int(uncom) {
				panic("Decompress1X")
			}
			if err != nil {
				panic(err)
			}

			n, err = w.Write(out)
			if n != len(out) {
				panic("Write to pipe failed")
			}
			if err != nil {
				panic(err)
			}
		}
	}
}
