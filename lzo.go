package extract

import (
	"encoding/binary"
	"github.com/rasky/go-lzo"
	"io"
)

var Uncompressed uint32
var Compressed uint32

func decompress(w io.Writer, s io.Reader) {
	var skip int = 33

	sk := make([]byte, skip)
	_, err := s.Read(sk)
	if err != nil {
		panic(err)
	}

	var fileNameLen uint8

	binary.Read(s, binary.BigEndian, &fileNameLen)

	fileName := make([]byte, fileNameLen)
	_, err = s.Read(fileName)
	if err != nil {
		panic(err)
	}

	fileComment := make([]byte, 4)
	_, err = s.Read(fileComment)
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
			io.CopyN(w, s, int64(com))

		} else {
			out, err := lzo.Decompress1X(s, int(com), int(uncom))
			if err != nil {
				panic(err)
			}

			_, err = w.Write(out)
			if err != nil {
				panic(err)
			}
		}
	}
}
