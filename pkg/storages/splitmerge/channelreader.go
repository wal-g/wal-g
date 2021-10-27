package splitmerge

import (
	"io"
)

type channelReader struct {
	in     <-chan []byte
	data   []byte
	offset int  // data[offset] - is first byte to read on next Read()
	ok     bool // when `false` - no more data
}

var _ io.ReadCloser = &channelReader{}

func NewChannelReader(in <-chan []byte) io.ReadCloser {
	return &channelReader{
		in: in,
	}
}

func (cr *channelReader) Read(dst []byte) (n int, err error) {
	dstOffset := 0
	for {
		if cr.offset >= len(cr.data) {
			cr.data, cr.ok = <-cr.in
			cr.offset = 0
		}
		if !cr.ok {
			//tracelog.InfoLogger.Printf("ChannelReader read finished [EOF]")
			return dstOffset, io.EOF
		}

		// `copy()` will copy min of slices sizes:
		copied := copy(dst[dstOffset:], cr.data[cr.offset:])
		cr.offset += copied
		dstOffset += copied

		if dstOffset >= len(dst) {
			return dstOffset, nil
		}
	}
}

func (cr *channelReader) Close() error {
	if cr.ok && len(cr.data) > cr.offset {
		panic("Not all data has been read")
	}
	return nil
}
