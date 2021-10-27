package splitmerge

import "io"

type channelWriter struct {
	ch        chan []byte
	block     []byte
	offset    int
	blockSize int
}

var _ io.WriteCloser = &channelWriter{}

func (cw *channelWriter) Write(data []byte) (int, error) {
	dataOffset := 0

	for {
		bytes := copy(cw.block[cw.offset:], data[dataOffset:])
		cw.offset += bytes
		dataOffset += bytes

		if cw.offset == len(cw.block) {
			cw.ch <- cw.block
			cw.block = make([]byte, cw.blockSize)
			cw.offset = 0
		}
		if dataOffset == len(data) {
			return len(data), nil

		}
	}
}

func (cw *channelWriter) Close() error {
	if cw.offset < len(cw.block) && cw.offset > 0 {
		cw.ch <- cw.block[:cw.offset]
	}
	close(cw.ch)
	return nil
}

func NewChannelWriter(ch chan []byte, blockSize int) io.WriteCloser {
	return &channelWriter{
		ch:        ch,
		block:     make([]byte, blockSize),
		blockSize: blockSize,
	}
}