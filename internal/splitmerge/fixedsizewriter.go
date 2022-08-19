package splitmerge

import "io"

type fixedBlockSizeWriter struct {
	dst       io.WriteCloser
	block     []byte
	offset    int
	blockSize int
}

var _ io.WriteCloser = &fixedBlockSizeWriter{}

func newFixedBlockSizeWriter(dst io.WriteCloser, blockSize int) io.WriteCloser {
	return &fixedBlockSizeWriter{
		dst:       dst,
		block:     make([]byte, blockSize),
		blockSize: blockSize,
	}
}

func (fbsw *fixedBlockSizeWriter) Write(data []byte) (int, error) {
	dataOffset := 0

	for {
		bytes := copy(fbsw.block[fbsw.offset:], data[dataOffset:])
		fbsw.offset += bytes
		dataOffset += bytes

		if fbsw.offset == len(fbsw.block) {
			wlen, err := fbsw.dst.Write(fbsw.block)
			if err != nil {
				fbsw.offset = 0
				return dataOffset - fbsw.blockSize + wlen, err
			}
			// Note: writer.Write() should never retain slice written to it
			// so, it is safe to reuse this buffer again.
			fbsw.offset = 0
		}
		if dataOffset == len(data) {
			return len(data), nil
		}
	}
}

func (fbsw *fixedBlockSizeWriter) Close() error {
	if fbsw.offset < len(fbsw.block) && fbsw.offset > 0 {
		_, err := fbsw.dst.Write(fbsw.block[:fbsw.offset])
		if err != nil {
			return err
		}
	}
	return fbsw.dst.Close()
}
