package utility

import "io"

type EOFProtectorReader struct {
	src       io.Reader
	firstByte byte
	read      bool
}

func NewEOFProtectorReader(reader io.Reader) (io.Reader, error) {
	firstByte := make([]byte, 1)
	read, err := reader.Read(firstByte)
	if err == io.EOF || err == io.ErrUnexpectedEOF || read == 0 {
		return nil, io.EOF
	} else {
		return &EOFProtectorReader{
			src:       reader,
			firstByte: firstByte[0],
			read:      false,
		}, nil
	}
}

func (r *EOFProtectorReader) Read(p []byte) (int, error) {
	if r.read {
		return r.src.Read(p)
	} else if len(p) >= 1 {
		p[0] = r.firstByte
		r.read = true
		n, err := r.src.Read(p[1:])
		return n + 1, err
	} else {
		return 0, nil
	}
}
